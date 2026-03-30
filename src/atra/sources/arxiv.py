from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import feedparser
import requests

from atra.db import PaperRow

ARXIV_API = "http://export.arxiv.org/api/query"


def _to_iso(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(dt_str)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        return None


@dataclass(frozen=True)
class ArxivIngestParams:
    category: str = "cs.AI"
    days: int = 1
    limit: int = 50


def fetch_arxiv(params: ArxivIngestParams) -> tuple[list[PaperRow], str]:
    if params.days < 1:
        raise ValueError("--days must be >= 1")
    if params.limit < 1 or params.limit > 2000:
        raise ValueError("--limit must be between 1 and 2000")

    q = f"cat:{params.category}"
    resp = requests.get(
        ARXIV_API,
        params={
            "search_query": q,
            "start": 0,
            "max_results": params.limit,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        },
        timeout=30,
        headers={"User-Agent": "ATRA-MInT/0.1 (research trend analyzer)"},
    )
    resp.raise_for_status()

    feed = feedparser.parse(resp.text)
    cutoff = datetime.now(timezone.utc) - timedelta(days=params.days)

    rows: list[PaperRow] = []
    for e in feed.entries:
        external_id = getattr(e, "id", None) or getattr(e, "link", None)
        if not external_id:
            continue

        published_at = _to_iso(getattr(e, "published", None))
        updated_at = _to_iso(getattr(e, "updated", None))

        dt_for_filter = None
        if published_at:
            dt_for_filter = datetime.fromisoformat(published_at)
        elif updated_at:
            dt_for_filter = datetime.fromisoformat(updated_at)

        if dt_for_filter and dt_for_filter < cutoff:
            continue

        authors = [a.name for a in getattr(e, "authors", []) if getattr(a, "name", None)]
        categories = []
        for t in getattr(e, "tags", []) or []:
            term = getattr(t, "term", None)
            if term:
                categories.append(term)

        rows.append(
            PaperRow(
                source="arxiv",
                external_id=str(external_id),
                url=str(getattr(e, "link", None) or ""),
                title=str(getattr(e, "title", "")).replace("\n", " ").strip(),
                abstract=str(getattr(e, "summary", "")).replace("\n", " ").strip() or None,
                published_at=published_at,
                updated_at=updated_at,
                authors_json=json.dumps(authors) if authors else None,
                categories_json=json.dumps(categories) if categories else None,
            )
        )

    params_json = json.dumps(
        {"category": params.category, "days": params.days, "limit": params.limit},
        ensure_ascii=False,
    )
    return rows, params_json
