"""
Shared daily job: multi-category arXiv + OpenAlex → summarize → tag → briefing.

Used by the CLI `atra daily` and optional Streamlit scheduled updates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from atra.db import connect, init_db, insert_run, upsert_papers
from atra.insights import generate_and_store_daily_insight
from atra.sources.arxiv import ArxivIngestParams, fetch_arxiv
from atra.sources.openalex import OpenAlexParams, fetch_openalex
from atra.summarize import summarize_missing
from atra.tagging import tag_missing_papers

ARXIV_DEFAULT_CATEGORIES = ("cs.AI", "cs.LG", "cs.CV", "q-bio.BM", "astro-ph.IM")


@dataclass(frozen=True)
class DailyRunResult:
    arxiv_categories: int
    openalex_rows: int
    inserted: int
    skipped: int
    summarized: int
    tagged: int


def run_daily(
    db_path: Path,
    *,
    days: int = 1,
    arxiv_limit: int = 25,
    openalex_limit: int = 50,
    openalex_search: Optional[str] = None,
    skip_insights: bool = False,
    arxiv_categories: Optional[tuple[str, ...]] = None,
    summarize_batch: int = 5000,
    tag_batch: int = 5000,
) -> tuple[DailyRunResult, Optional[dict[str, Any]]]:
    """Full ingest → summarize → tag → optional stored briefing."""
    cats = arxiv_categories if arxiv_categories is not None else ARXIV_DEFAULT_CATEGORIES
    init_db(db_path)
    total_ins = 0
    total_sk = 0
    for cat in cats:
        rows, params_json = fetch_arxiv(ArxivIngestParams(category=cat, days=days, limit=arxiv_limit))
        con = connect(db_path)
        try:
            insert_run(con, source="arxiv", params_json=params_json)
            ins, sk = upsert_papers(con, rows)
            con.commit()
            total_ins += ins
            total_sk += sk
        finally:
            con.close()

    oa_rows, oa_params = fetch_openalex(
        OpenAlexParams(days=days, limit=min(openalex_limit, 200), search=openalex_search)
    )
    con = connect(db_path)
    try:
        insert_run(con, source="openalex", params_json=oa_params)
        ins, sk = upsert_papers(con, oa_rows)
        con.commit()
        total_ins += ins
        total_sk += sk
    finally:
        con.close()

    s = summarize_missing(db_path, batch_limit=summarize_batch)
    t = tag_missing_papers(db_path, batch_limit=tag_batch)
    payload: Optional[dict[str, Any]] = None
    if not skip_insights:
        payload = generate_and_store_daily_insight(db_path)
    result = DailyRunResult(
        arxiv_categories=len(cats),
        openalex_rows=len(oa_rows),
        inserted=total_ins,
        skipped=total_sk,
        summarized=s,
        tagged=t,
    )
    return result, payload


def hours_since_last_insert(db_path: Path) -> Optional[float]:
    """None if no papers; else hours since latest inserted_at (UTC-aware where possible)."""
    init_db(db_path)
    con = connect(db_path)
    try:
        row = con.execute("SELECT MAX(inserted_at) AS m FROM papers").fetchone()
        raw = row["m"] if row else None
    finally:
        con.close()
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
        return max(0.0, delta.total_seconds() / 3600.0)
    except (TypeError, ValueError):
        return None
