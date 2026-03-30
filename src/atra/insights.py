"""
Daily intelligence briefing: trend spikes, emerging keywords, priority items.

Designed to run after ingestion (see `atra daily`). Results persist in `daily_insights`.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from atra.db import (
    DEFAULT_DB_PATH,
    connect,
    count_papers,
    init_db,
    save_daily_insight,
)

_STOP = re.compile(
    r"\b(the|a|an|and|or|for|to|of|in|on|with|by|we|our|this|that|is|are|as|be|from|at|it|using|use)\b",
    re.I,
)


def _tokens(blob: str, *, min_len: int = 4) -> list[str]:
    tokens = re.findall(r"[a-zA-Z][a-zA-Z\-]{2,}", (blob or "").lower())
    out: list[str] = []
    for t in tokens:
        if len(t) < min_len:
            continue
        if _STOP.search(f" {t} "):
            continue
        out.append(t)
    return out


def _day_from_row(published_at: Optional[str], inserted_at: Optional[str]) -> Optional[str]:
    if published_at and len(str(published_at)) >= 10:
        return str(published_at)[:10]
    if inserted_at and len(str(inserted_at)) >= 10:
        return str(inserted_at)[:10]
    return None


def _parse_dt(iso: str) -> Optional[datetime]:
    try:
        s = iso.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _sectors_from_json(sj: str) -> list[str]:
    try:
        items = json.loads(sj) if sj else []
    except json.JSONDecodeError:
        return []
    return [str(it.get("sector") or "Unclassified") for it in items[:6]]


def compute_daily_insights(db_path: Path | None = None) -> dict[str, Any]:
    """Build structured briefing for *today* (local date) and return dict (also caller may persist)."""
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    try:
        rows = list(
            con.execute(
                """
                SELECT id, title, abstract, summary, published_at, inserted_at,
                       relevance_et, impact_level, sectors_json, url, source
                FROM papers
                ORDER BY datetime(inserted_at) DESC
                LIMIT 8000
                """
            ).fetchall()
        )
        total_db = count_papers(con)
    finally:
        con.close()

    today = date.today().isoformat()
    now_utc = datetime.now(timezone.utc)
    cutoff_24h = now_utc - timedelta(hours=24)
    cutoff_36h = now_utc - timedelta(hours=36)

    def _inserted_utc(r: Any) -> Optional[datetime]:
        ins = _parse_dt(str(r["inserted_at"]))
        if not ins:
            return None
        if ins.tzinfo is None:
            ins = ins.replace(tzinfo=timezone.utc)
        return ins.astimezone(timezone.utc)

    # --- ingest freshness (last 24h by inserted_at)
    recent_rows: list[Any] = []
    for r in rows:
        ins = _inserted_utc(r)
        if ins and ins >= cutoff_24h:
            recent_rows.append(r)

    new_24h = len(recent_rows)

    # --- sector × calendar day (published/inserted day)
    day_sector_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        d = _day_from_row(r["published_at"], r["inserted_at"])
        if not d:
            continue
        for sec in _sectors_from_json(r["sectors_json"] or ""):
            day_sector_counts[d][sec] += 1

    # baseline: previous 7 calendar days before today
    d0 = date.fromisoformat(today)
    baseline_days = [(d0 - timedelta(days=i)).isoformat() for i in range(1, 8)]
    sector_baseline: dict[str, float] = defaultdict(float)
    for bd in baseline_days:
        for sec, c in day_sector_counts.get(bd, {}).items():
            sector_baseline[sec] += c
    for sec in list(sector_baseline.keys()):
        sector_baseline[sec] = sector_baseline[sec] / 7.0

    today_sec = day_sector_counts.get(today, {})
    sector_momentum: list[dict[str, Any]] = []
    for sec, tcount in sorted(today_sec.items(), key=lambda x: -x[1]):
        base = sector_baseline.get(sec, 0.25)
        ratio = tcount / (base + 0.01)
        if tcount == 0:
            continue
        if ratio >= 1.6 and tcount >= 2:
            signal = "surge"
        elif ratio >= 1.25:
            signal = "up"
        elif tcount >= 4 and base < 1:
            signal = "hot"
        else:
            signal = "steady"
        sector_momentum.append(
            {
                "sector": sec,
                "today": tcount,
                "baseline_daily_avg": round(base, 2),
                "ratio_vs_baseline": round(ratio, 2),
                "signal": signal,
            }
        )
    sector_momentum.sort(key=lambda x: (-x["today"], -x["ratio_vs_baseline"]))

    # --- emerging keywords: recent 36h vs prior window (days 3–12 ago)
    prior_start = (d0 - timedelta(days=12)).isoformat()
    prior_end = (d0 - timedelta(days=3)).isoformat()

    def in_prior_window(d: Optional[str]) -> bool:
        if not d:
            return False
        return prior_start <= d <= prior_end

    blob_recent = " ".join(
        f"{r['title'] or ''} {r['abstract'] or ''} {r['summary'] or ''}"
        for r in rows
        if (d := _inserted_utc(r)) is not None and d >= cutoff_36h
    )
    blob_prior = " ".join(
        f"{r['title'] or ''} {r['abstract'] or ''} {r['summary'] or ''}"
        for r in rows
        if in_prior_window(_day_from_row(r["published_at"], r["inserted_at"]))
    )
    c_recent = Counter(_tokens(blob_recent))
    c_prior = Counter(_tokens(blob_prior))
    emerging: list[dict[str, Any]] = []
    for tok, rc in c_recent.most_common(80):
        if rc < 2:
            continue
        pv = c_prior.get(tok, 0)
        lift = (rc + 0.5) / (pv + 0.5)
        if lift >= 1.8 or (rc >= 4 and pv == 0):
            emerging.append(
                {"token": tok, "recent_count": rc, "prior_count": pv, "lift": round(lift, 2)}
            )
    emerging.sort(key=lambda x: (-x["lift"], -x["recent_count"]))
    emerging = emerging[:20]

    # --- priority brief: best Ethiopia relevance among recently ingested
    priority: list[dict[str, Any]] = []
    for r in sorted(
        recent_rows,
        key=lambda x: (x["relevance_et"] is None, -(x["relevance_et"] or 0)),
    )[:12]:
        priority.append(
            {
                "id": r["id"],
                "title": r["title"],
                "summary": (r["summary"] or "")[:400],
                "relevance_et": r["relevance_et"],
                "impact_level": r["impact_level"],
                "url": r["url"],
                "source": r["source"],
            }
        )

    # --- narrative bullets for executives
    bullets: list[str] = []
    bullets.append(
        f"In the last 24 hours, {new_24h} new research items were indexed (total corpus: {total_db})."
    )
    if sector_momentum and sector_momentum[0]["signal"] in ("surge", "hot", "up"):
        top = sector_momentum[0]
        bullets.append(
            f"Sector watch: {top['sector']} shows elevated activity "
            f"({top['today']} today vs ~{top['baseline_daily_avg']} / day baseline over the past week)."
        )
    if emerging[:3]:
        toks = ", ".join(e["token"] for e in emerging[:5])
        bullets.append(f"Emerging terms in fresh literature: {toks}.")
    if priority:
        bullets.append(
            f"Top Ethiopia-relevance lead: {priority[0]['title'][:120]}{'…' if len(str(priority[0]['title'])) > 120 else ''}"
        )
    if not bullets:
        bullets.append("Run a broader ingest (`atra daily`) to populate intelligence signals.")

    payload: dict[str, Any] = {
        "generated_at": now_utc.replace(microsecond=0).isoformat(),
        "headline_stats": {
            "new_items_24h": new_24h,
            "total_papers": total_db,
            "report_calendar_date": today,
        },
        "sector_momentum": sector_momentum[:15],
        "emerging_keywords": emerging,
        "priority_brief": priority,
        "narrative_bullets": bullets,
    }
    return payload


def generate_and_store_daily_insight(db_path: Path | None = None) -> dict[str, Any]:
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    payload = compute_daily_insights(path)
    report_date = payload["headline_stats"]["report_calendar_date"]
    con = connect(path)
    try:
        save_daily_insight(con, report_for_date=report_date, payload=payload)
        con.commit()
    finally:
        con.close()
    return payload
