from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from atra.db import DEFAULT_DB_PATH, connect, init_db

# Ethiopia-relevant sectors (MInT / Digital 2025–30 alignment)
SECTOR_KEYWORDS: dict[str, list[str]] = {
    "Agriculture": [
        "agriculture",
        "agricultural",
        "crop",
        "livestock",
        "irrigation",
        "farming",
        "food security",
        "soil",
        "drought",
        "teff",
        "coffee",
        "pastoral",
    ],
    "Health": [
        "health",
        "medical",
        "clinical",
        "disease",
        "malaria",
        "tuberculosis",
        "vaccine",
        "epidemic",
        "patient",
        "diagnosis",
        "telemedicine",
        "public health",
    ],
    "Manufacturing": [
        "manufacturing",
        "factory",
        "industrial",
        "supply chain",
        "automation",
        "robot",
        "3d print",
        "additive manufacturing",
    ],
    "Energy": [
        "energy",
        "solar",
        "wind power",
        "grid",
        "battery",
        "hydropower",
        "renewable",
        "electricity",
    ],
    "Telecom": [
        "telecom",
        "5g",
        "6g",
        "wireless",
        "network",
        "broadband",
        "internet",
        "satellite communication",
    ],
    "Education": [
        "education",
        "learning",
        "student",
        "curriculum",
        "edtech",
        "literacy",
    ],
    "Space": [
        "space",
        "satellite",
        "orbit",
        "earth observation",
        "remote sensing",
        "astro",
        "planetary",
    ],
    "AI & ICT": [
        "artificial intelligence",
        "machine learning",
        "deep learning",
        "nlp",
        "large language model",
        "computer vision",
        "software",
        "cybersecurity",
        "digital",
        "ict",
    ],
    "Biotech": [
        "biotech",
        "genomics",
        "gene",
        "protein",
        "synthetic biology",
        "crispr",
        "pharmaceutical",
    ],
    "Public services": [
        "government",
        "public sector",
        "civil service",
        "e-government",
        "digital id",
        "policy",
        "governance",
    ],
}

# Signals that tie global tech to Ethiopian / low-resource / emerging-economy context
ETHIOPIA_CONTEXT_KEYWORDS = [
    "ethiopia",
    "ethiopian",
    "addis",
    "horn of africa",
    "east africa",
    "africa",
    "developing countr",
    "low-resource",
    "low resource",
    "global south",
    "rural",
    "smallholder",
]


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def score_sectors(text: str) -> list[dict[str, Any]]:
    t = _normalize(text)
    scores: list[dict[str, Any]] = []
    for sector, kws in SECTOR_KEYWORDS.items():
        raw = 0.0
        hits = 0
        for kw in kws:
            if kw.lower() in t:
                raw += 1.0
                hits += 1
        # Cap contribution per sector
        s = min(1.0, raw / max(3.0, len(kws) / 5.0))
        if s > 0.05:
            scores.append({"sector": sector, "score": round(s, 3), "hits": hits})
    scores.sort(key=lambda x: -x["score"])
    return scores


def ethiopia_relevance(text: str, sector_scores: list[dict[str, Any]]) -> float:
    """0..1 rough relevance to national prioritization."""
    t = _normalize(text)
    ctx = 0.0
    for kw in ETHIOPIA_CONTEXT_KEYWORDS:
        if kw in t:
            ctx += 0.12
    ctx = min(1.0, ctx)
    sector_part = min(1.0, sum(s["score"] for s in sector_scores[:3]) / 2.5) if sector_scores else 0.0
    # Blend: sector fit + regional / development context
    rel = min(1.0, 0.55 * sector_part + 0.45 * ctx)
    return round(rel, 3)


def impact_level(relevance: float, cited_by_count: int | None) -> str:
    c = cited_by_count or 0
    score = relevance * 0.6 + min(1.0, c / 100.0) * 0.4
    if score >= 0.55:
        return "high"
    if score >= 0.28:
        return "medium"
    return "low"


def tag_text_bundle(
    title: str,
    abstract: str | None,
    summary: str | None,
    cited_by_count: int | None,
) -> tuple[float, str, str]:
    blob = f"{title}\n{abstract or ''}\n{summary or ''}"
    sectors = score_sectors(blob)
    rel = ethiopia_relevance(blob, sectors)
    imp = impact_level(rel, cited_by_count)
    sectors_json = json.dumps(sectors[:8], ensure_ascii=False)
    return rel, imp, sectors_json


def tag_missing_papers(db_path: Path | None = None, batch_limit: int = 500) -> int:
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    updated = 0
    try:
        cur = con.execute(
            """
            SELECT id, title, abstract, summary, cited_by_count
            FROM papers
            WHERE sectors_json IS NULL OR sectors_json = ''
            LIMIT ?
            """,
            (batch_limit,),
        )
        for row in cur.fetchall():
            rel, imp, sj = tag_text_bundle(
                str(row["title"]),
                row["abstract"],
                row["summary"],
                row["cited_by_count"],
            )
            con.execute(
                """
                UPDATE papers
                SET relevance_et = ?, impact_level = ?, sectors_json = ?
                WHERE id = ?
                """,
                (rel, imp, sj, int(row["id"])),
            )
            updated += 1
        con.commit()
    finally:
        con.close()
    return updated


def re_tag_all(db_path: Path | None = None, batch_limit: int = 2000) -> int:
    """Overwrite tags for papers (up to batch_limit, oldest first)."""
    path = db_path or DEFAULT_DB_PATH
    init_db(path)
    con = connect(path)
    updated = 0
    try:
        cur = con.execute(
            """
            SELECT id, title, abstract, summary, cited_by_count
            FROM papers ORDER BY id ASC LIMIT ?
            """,
            (batch_limit,),
        )
        for row in cur.fetchall():
            rel, imp, sj = tag_text_bundle(
                str(row["title"]),
                row["abstract"],
                row["summary"],
                row["cited_by_count"],
            )
            con.execute(
                """
                UPDATE papers
                SET relevance_et = ?, impact_level = ?, sectors_json = ?
                WHERE id = ?
                """,
                (rel, imp, sj, int(row["id"])),
            )
            updated += 1
        con.commit()
    finally:
        con.close()
    return updated


def list_sector_names() -> list[str]:
    return sorted(SECTOR_KEYWORDS.keys())
