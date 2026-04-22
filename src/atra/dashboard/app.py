"""
ATRA decision dashboard (Streamlit).

Run: streamlit run src/atra/dashboard/app.py
Or from repo root with package installed: streamlit run -m atra.dashboard.app
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

from atra.daily_pipeline import hours_since_last_insert, run_daily
from atra.db import connect, get_latest_daily_insight, init_db, insert_run, query_papers, upsert_papers
from atra.insights import generate_and_store_daily_insight
from atra.summarize import summarize_missing
from atra.sources.arxiv import ArxivIngestParams, fetch_arxiv
from atra.tagging import list_sector_names, tag_missing_papers
from atra.trends import early_signals, sector_trend_series, top_tokens


def db_path() -> Path:
    return Path(os.environ.get("ATRA_DB_PATH", "data/atra.db"))


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _auto_daily_ingest_enabled() -> bool:
    return os.environ.get("ATRA_AUTO_DAILY_INGEST", "").strip().lower() in ("1", "true", "yes", "on")


def _run_daily_with_env_limits(path: Path) -> None:
    """Same job as `python -m atra daily` with limits from environment (tunable for Streamlit timeouts)."""
    init_db(path)
    run_daily(
        path,
        days=_env_int("ATRA_DAILY_DAYS", 1),
        arxiv_limit=_env_int("ATRA_DAILY_ARXIV_LIMIT", 15),
        openalex_limit=_env_int("ATRA_DAILY_OPENALEX_LIMIT", 30),
    )


def _bootstrap_from_arxiv(path: Path, *, category: str, days: int, limit: int) -> tuple[int, int]:
    """Ingest → summarize → tag → briefing (small pull suitable for Streamlit Cloud)."""
    rows, params_json = fetch_arxiv(ArxivIngestParams(category=category, days=days, limit=limit))
    con = connect(path)
    try:
        insert_run(con, source="arxiv", params_json=params_json)
        inserted, skipped = upsert_papers(con, rows)
        con.commit()
    finally:
        con.close()
    summarize_missing(path, batch_limit=min(500, limit * 20))
    tag_missing_papers(path, batch_limit=min(500, limit * 20))
    generate_and_store_daily_insight(path)
    return inserted, skipped


def _ensure_stored_briefing() -> None:
    """Hosted apps never run `atra daily`; create a briefing row if the table is empty."""
    path = db_path()
    init_db(path)
    con = connect(path)
    try:
        if get_latest_daily_insight(con) is None:
            generate_and_store_daily_insight(path)
    finally:
        con.close()


@st.cache_data(ttl=120)
def load_latest_briefing() -> dict | None:
    init_db(db_path())
    con = connect(db_path())
    try:
        return get_latest_daily_insight(con)
    finally:
        con.close()


@st.fragment(run_every=timedelta(hours=8))
def _scheduled_daily_ingest() -> None:
    """When ATRA_AUTO_DAILY_INGEST=1, periodically ingest if the last paper is old enough."""
    if not _auto_daily_ingest_enabled():
        return
    path = db_path()
    min_h = _env_float("ATRA_DAILY_MIN_INTERVAL_HOURS", 18.0)
    h = hours_since_last_insert(path)
    if h is not None and h < min_h:
        return
    try:
        with st.spinner("Scheduled daily ingest — updating papers and trends…"):
            _run_daily_with_env_limits(path)
        load_latest_briefing.clear()
        st.rerun()
    except (OSError, requests.RequestException):
        return


st.set_page_config(page_title="ATRA — MInT", layout="wide")
st.title("ATRA — Tech trend & research intelligence")
st.caption(
    "Ministry of Innovation and Technology · Daily briefing refreshes from the database every ~2 minutes while this page is open."
)

init_db(db_path())
_ensure_stored_briefing()

with st.sidebar:
    st.subheader("Load data")
    st.caption(
        "Cloud deploys start with an empty database. Fetch a small **cs.AI** slice from arXiv, "
        "then summarize, tag, and refresh the briefing (~30–90s)."
    )
    if st.button("Fetch sample papers from arXiv"):
        path = db_path()
        init_db(path)
        try:
            with st.spinner("Fetching from arXiv…"):
                ins, sk = _bootstrap_from_arxiv(path, category="cs.AI", days=7, limit=25)
            load_latest_briefing.clear()
            st.success(f"Stored {ins} new papers ({sk} duplicates skipped). Refreshing…")
        except (OSError, requests.RequestException) as exc:
            st.error(f"Could not reach arXiv or save the database: {exc}")
            st.stop()
        st.rerun()
    st.divider()
    st.subheader("Daily briefing")
    st.caption("Uses papers already in the database (does not fetch new articles).")
    if st.button("Regenerate briefing"):
        generate_and_store_daily_insight(db_path())
        load_latest_briefing.clear()
        st.success("Briefing updated.")
        st.rerun()
    st.divider()
    st.subheader("Daily trend updates")
    st.caption(
        "Trends use whatever is in the database. **Run full daily update** pulls all default arXiv "
        "categories + OpenAlex, then re-tags and refreshes the briefing (several minutes). "
        "For automatic runs while the app is open, set **ATRA_AUTO_DAILY_INGEST=1** in Streamlit "
        "secrets and tune **ATRA_DAILY_MIN_INTERVAL_HOURS** (default 18), **ATRA_DAILY_ARXIV_LIMIT**, "
        "**ATRA_DAILY_OPENALEX_LIMIT**."
    )
    if st.button("Run full daily update now"):
        path = db_path()
        init_db(path)
        try:
            with st.spinner("Running daily pipeline (ingest → summarize → tag → briefing)…"):
                _run_daily_with_env_limits(path)
            load_latest_briefing.clear()
            st.success("Daily update finished. Refreshing…")
        except (OSError, requests.RequestException) as exc:
            st.error(f"Daily update failed (network or disk): {exc}")
            st.stop()
        st.rerun()
    st.divider()
    st.header("Filters")
    date_from = st.text_input("Date from (YYYY-MM-DD)", "")
    date_to = st.text_input("Date to (YYYY-MM-DD)", "")
    sector = st.selectbox("Sector", [""] + list_sector_names())
    impact = st.selectbox("Impact", ["", "low", "medium", "high"])
    source = st.selectbox("Source", ["", "arxiv", "openalex"])
    search = st.text_input("Search (title / abstract / summary)", "")
    limit = st.slider("Max rows", 10, 300, 80)

_scheduled_daily_ingest()

con = connect(db_path())
try:
    papers = query_papers(
        con,
        date_from=date_from or None,
        date_to=date_to or None,
        sector=sector or None,
        impact=impact or None,
        source=source or None,
        search=search or None,
        limit=limit,
        offset=0,
    )
finally:
    con.close()

tab0, tab1, tab2, tab3 = st.tabs(
    ["Daily briefing", "Papers", "Trends", "Early signals"]
)

with tab0:
    briefing = load_latest_briefing()
    if not briefing:
        st.warning(
            "No briefing could be loaded. Check that the database path is writable and try "
            "**Regenerate briefing** in the sidebar, or run **`python -m atra daily`** / **`python -m atra insights`** "
            "where the SQLite file lives."
        )
    else:
        st.subheader(f"Report date: {briefing.get('report_for_date', '—')}")
        st.caption(
            f"Generated: {briefing.get('generated_at') or briefing.get('stored_generated_at', '—')}"
        )
        hs = briefing.get("headline_stats") or {}
        c1, c2, c3 = st.columns(3)
        c1.metric("New items (24h)", hs.get("new_items_24h", "—"))
        c2.metric("Corpus size", hs.get("total_papers", "—"))
        c3.metric("Calendar date", hs.get("report_calendar_date", "—"))

        st.markdown("### Executive bullets")
        for b in briefing.get("narrative_bullets") or []:
            st.markdown(f"- {b}")

        mom = briefing.get("sector_momentum") or []
        if mom:
            st.markdown("### Sector momentum")
            st.dataframe(pd.DataFrame(mom), width="stretch", hide_index=True)

        em = briefing.get("emerging_keywords") or []
        if em:
            st.markdown("### Emerging keywords (vs prior week)")
            st.bar_chart(pd.DataFrame(em).set_index("token")["lift"])

        pb = briefing.get("priority_brief") or []
        if pb:
            st.markdown("### Priority brief (Ethiopia relevance)")
            st.dataframe(pd.DataFrame(pb), width="stretch", hide_index=True)

with tab1:
    if not papers:
        st.info(
            "No papers in the database yet. Use **Fetch sample papers from arXiv** in the sidebar, "
            "or run **`python -m atra daily`** where the SQLite file is stored."
        )
    else:
        rows = []
        for p in papers:
            sectors = p.get("sectors_json") or ""
            try:
                sj = json.loads(sectors) if sectors else []
                sec_txt = ", ".join(f"{x.get('sector','')} ({x.get('score','')})" for x in sj[:4])
            except json.JSONDecodeError:
                sec_txt = sectors[:120]
            rows.append(
                {
                    "id": p["id"],
                    "date": (p.get("published_at") or "")[:10],
                    "impact": p.get("impact_level"),
                    "ET relevance": p.get("relevance_et"),
                    "title": p.get("title"),
                    "sectors": sec_txt,
                    "source": p.get("source"),
                    "url": p.get("url"),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df, width="stretch", hide_index=True)

with tab2:
    st.caption(
        "Sector and keyword trends reflect **ingested papers**. Use **Run full daily update** or "
        "scheduled ingest so this view gains new days of activity."
    )
    series = sector_trend_series(db_path())
    if series:
        sdf = pd.DataFrame(series)
        try:
            pivot = sdf.pivot_table(
                index="date", columns="sector", values="count", aggfunc="sum"
            ).fillna(0)
            st.subheader("Activity by sector (daily)")
            st.line_chart(pivot)
        except Exception:
            st.dataframe(sdf, width="stretch", hide_index=True)
    else:
        st.info("No trend data yet.")

    kw = top_tokens(db_path(), top_n=25)
    if kw:
        st.subheader("Top keywords (recent papers)")
        st.bar_chart(pd.DataFrame(kw).set_index("token"))

with tab3:
    sigs = early_signals(db_path(), recent_days=14)
    if not sigs:
        st.info("No high-priority signals in the recent window.")
    else:
        st.dataframe(pd.DataFrame(sigs), width="stretch", hide_index=True)
