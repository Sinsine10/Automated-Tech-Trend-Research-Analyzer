from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

import requests

from atra.db import PaperRow

OPENALEX_WORKS = "https://api.openalex.org/works"


def _mailto() -> str:
    return os.environ.get("ATRA_CONTACT_EMAIL", "atra@example.org")


def reconstruct_abstract(inv_index: Optional[dict[str, list[int]]]) -> Optional[str]:
    if not inv_index:
        return None
    pairs: list[tuple[int, str]] = []
    for word, positions in inv_index.items():
        for pos in positions or []:
            pairs.append((int(pos), word))
    pairs.sort(key=lambda x: x[0])
    return " ".join(w for _, w in pairs).strip() or None


def _iso_date(d: date) -> str:
    return d.isoformat()


@dataclass(frozen=True)
class OpenAlexParams:
    days: int = 7
    limit: int = 50
    search: Optional[str] = None
    """Keyword search (OpenAlex `search` param)."""


def fetch_openalex(params: OpenAlexParams) -> tuple[list[PaperRow], str]:
    if params.days < 1:
        raise ValueError("days must be >= 1")
    if params.limit < 1 or params.limit > 200:
        raise ValueError("limit must be 1..200 per request")

    from_day = date.today() - timedelta(days=params.days)
    q: dict[str, Any] = {
        "filter": f"from_publication_date:{_iso_date(from_day)}",
        "per_page": min(params.limit, 200),
        "mailto": _mailto(),
    }
    if params.search:
        q["search"] = params.search

    resp = requests.get(
        OPENALEX_WORKS,
        params=q,
        timeout=60,
        headers={"User-Agent": f"ATRA/1.0 (mailto:{_mailto()})"},
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results") or []

    rows: list[PaperRow] = []
    for w in results:
        wid = w.get("id") or ""
        if not wid:
            continue
        title = (w.get("display_name") or "").strip() or "(untitled)"
        abstract = reconstruct_abstract(w.get("abstract_inverted_index"))
        pub = w.get("publication_date") or None
        published_at = f"{pub}T00:00:00+00:00" if pub else None

        authors: list[str] = []
        for a in w.get("authorships") or []:
            inst = a.get("author") or {}
            name = inst.get("display_name")
            if name:
                authors.append(name)

        primary = (w.get("primary_location") or {}).get("landing_page_url") or ""
        doi = w.get("doi") or ""
        url = primary or (doi if str(doi).startswith("http") else f"https://doi.org/{doi}" if doi else wid)

        cited = w.get("cited_by_count")
        try:
            cc = int(cited) if cited is not None else None
        except (TypeError, ValueError):
            cc = None

        concepts = [c.get("display_name") for c in (w.get("concepts") or [])[:15] if c.get("display_name")]

        rows.append(
            PaperRow(
                source="openalex",
                external_id=str(wid),
                url=url or None,
                title=title,
                abstract=abstract,
                published_at=published_at,
                updated_at=None,
                authors_json=json.dumps(authors, ensure_ascii=False) if authors else None,
                categories_json=json.dumps(concepts, ensure_ascii=False) if concepts else None,
                cited_by_count=cc,
            )
        )

    meta = {
        "days": params.days,
        "limit": params.limit,
        "search": params.search,
        "count": len(rows),
    }
    return rows, json.dumps(meta, ensure_ascii=False)
