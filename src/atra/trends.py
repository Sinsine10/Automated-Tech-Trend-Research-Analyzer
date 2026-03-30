from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from atra.db import DEFAULT_DB_PATH, connect, init_db, papers_for_trends


def _day_key(iso_ts: str | None) -> str | None:
    if not iso_ts:
        return None
    s = str(iso_ts).strip()
    if len(s) >= 10:
        return s[:10]
    return None


def sector_trend_series(db_path: Path | None = None) -> list[dict[str, Any]]:
    """Counts per calendar day × sector from sectors_json."""
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    try:
        rows = papers_for_trends(con)
    finally:
        con.close()

    # day -> sector -> count
    nested: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        day = _day_key(r["published_at"]) or _day_key(r["inserted_at"])
        if not day:
            continue
        sj = r["sectors_json"] or ""
        try:
            items = json.loads(sj) if sj else []
        except json.JSONDecodeError:
            items = []
        if not items:
            nested[day]["Unclassified"] += 1
            continue
        for it in items[:5]:
            sec = it.get("sector") or "Unclassified"
            nested[day][sec] += 1

    out: list[dict[str, Any]] = []
    for day in sorted(nested.keys()):
        for sector, c in sorted(nested[day].items(), key=lambda x: -x[1]):
            out.append({"date": day, "sector": sector, "count": c})
    return out


_STOP = re.compile(
    r"\b(the|a|an|and|or|for|to|of|in|on|with|by|we|our|this|that|is|are|as|be|from|at|it|using|use)\b",
    re.I,
)


def top_tokens(
    db_path: Path | None = None,
    max_papers: int = 2000,
    top_n: int = 30,
    min_len: int = 4,
) -> list[dict[str, Any]]:
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    try:
        rows = list(
            con.execute(
                """
                SELECT title, abstract, summary FROM papers
                ORDER BY datetime(COALESCE(published_at, inserted_at)) DESC
                LIMIT ?
                """,
                (max_papers,),
            ).fetchall()
        )
    finally:
        con.close()

    blob = " ".join(
        f"{r['title'] or ''} {r['abstract'] or ''} {r['summary'] or ''}" for r in rows
    )
    tokens = re.findall(r"[a-zA-Z][a-zA-Z\-]{2,}", blob.lower())
    cleaned = []
    for t in tokens:
        if len(t) < min_len:
            continue
        if _STOP.search(f" {t} "):
            continue
        cleaned.append(t)
    counts = Counter(cleaned)
    return [{"token": w, "count": c} for w, c in counts.most_common(top_n)]


def early_signals(db_path: Path | None = None, recent_days: int = 14) -> list[dict[str, Any]]:
    """Papers in the window with high Ethiopia relevance or high impact."""
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    try:
        rows = list(
            con.execute(
                """
                SELECT id, title, published_at, relevance_et, impact_level, sectors_json, url, source
                FROM papers
                ORDER BY datetime(COALESCE(published_at, inserted_at)) DESC
                LIMIT 500
                """
            ).fetchall()
        )
    finally:
        con.close()

    cutoff = None
    try:
        from datetime import date, timedelta

        cutoff = (date.today() - timedelta(days=recent_days)).isoformat()
    except Exception:
        pass

    signals: list[dict[str, Any]] = []
    for r in rows:
        day = _day_key(r["published_at"])
        if cutoff and day and day < cutoff:
            continue
        rel = r["relevance_et"]
        imp = (r["impact_level"] or "").lower()
        if (rel is not None and rel >= 0.45) or imp == "high":
            signals.append(
                {
                    "id": r["id"],
                    "title": r["title"],
                    "published_at": r["published_at"],
                    "relevance_et": rel,
                    "impact_level": r["impact_level"],
                    "url": r["url"],
                    "source": r["source"],
                }
            )
    return signals[:50]
