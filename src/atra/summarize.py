from __future__ import annotations

import re
from pathlib import Path

from atra.db import DEFAULT_DB_PATH, connect, init_db

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def simple_3_sentence_summary(text: str, *, max_chars: int = 600) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    sentences = SENTENCE_SPLIT_RE.split(text)
    sentences = [s.strip() for s in sentences if s.strip()]
    selected = " ".join(sentences[:3])
    if len(selected) > max_chars:
        selected = selected[: max_chars - 1].rsplit(" ", 1)[0] + "…"
    return selected


def summarize_missing(
    db_path: Path | str | None = None,
    batch_limit: int = 200,
) -> int:
    path = DEFAULT_DB_PATH if db_path is None else Path(db_path)
    init_db(path)
    con = connect(path)
    try:
        cur = con.execute(
            """
            SELECT id, abstract
            FROM papers
            WHERE (summary IS NULL OR summary = '')
              AND abstract IS NOT NULL
            LIMIT ?
            """,
            (batch_limit,),
        )
        rows = list(cur.fetchall())
        if not rows:
            return 0

        updated = 0
        for r in rows:
            summary = simple_3_sentence_summary(str(r["abstract"]))
            if not summary:
                continue
            con.execute(
                "UPDATE papers SET summary = ? WHERE id = ?",
                (summary, int(r["id"])),
            )
            updated += 1
        con.commit()
        return updated
    finally:
        con.close()
