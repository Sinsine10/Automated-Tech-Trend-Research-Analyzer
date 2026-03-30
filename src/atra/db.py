from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


DEFAULT_DB_PATH = Path("data") / "atra.db"

# Columns added after v0.1 — applied via _migrate()
_EXTRA_COLUMNS = [
    ("relevance_et", "REAL"),
    ("impact_level", "TEXT"),
    ("sectors_json", "TEXT"),
    ("cited_by_count", "INTEGER"),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def _migrate(con: sqlite3.Connection) -> None:
    for col, coltype in _EXTRA_COLUMNS:
        try:
            con.execute(f"ALTER TABLE papers ADD COLUMN {col} {coltype};")
        except sqlite3.OperationalError:
            pass
    try:
        con.execute("ALTER TABLE papers ADD COLUMN summary TEXT;")
    except sqlite3.OperationalError:
        pass


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    con = connect(db_path)
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at TEXT NOT NULL,
              source TEXT NOT NULL,
              params_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS papers (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source TEXT NOT NULL,
              external_id TEXT NOT NULL,
              url TEXT,
              title TEXT NOT NULL,
              abstract TEXT,
              published_at TEXT,
              updated_at TEXT,
              authors_json TEXT,
              categories_json TEXT,
              summary TEXT,
              relevance_et REAL,
              impact_level TEXT,
              sectors_json TEXT,
              cited_by_count INTEGER,
              inserted_at TEXT NOT NULL,
              UNIQUE(source, external_id)
            );

            CREATE INDEX IF NOT EXISTS idx_papers_source_published
              ON papers(source, published_at);

            CREATE TABLE IF NOT EXISTS daily_insights (
              report_for_date TEXT NOT NULL PRIMARY KEY,
              generated_at TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            """
        )
        _migrate(con)
        try:
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_papers_impact ON papers(impact_level);"
            )
        except sqlite3.OperationalError:
            pass
        con.commit()
    finally:
        con.close()


@dataclass(frozen=True)
class PaperRow:
    source: str
    external_id: str
    url: Optional[str]
    title: str
    abstract: Optional[str]
    published_at: Optional[str]
    updated_at: Optional[str]
    authors_json: Optional[str]
    categories_json: Optional[str]
    summary: Optional[str] = None
    relevance_et: Optional[float] = None
    impact_level: Optional[str] = None
    sectors_json: Optional[str] = None
    cited_by_count: Optional[int] = None


def insert_run(con: sqlite3.Connection, *, source: str, params_json: str) -> int:
    cur = con.execute(
        "INSERT INTO runs(started_at, source, params_json) VALUES (?, ?, ?)",
        (utc_now_iso(), source, params_json),
    )
    return int(cur.lastrowid)


def upsert_papers(con: sqlite3.Connection, rows: Iterable[PaperRow]) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    for r in rows:
        try:
            con.execute(
                """
                INSERT INTO papers(
                  source, external_id, url, title, abstract,
                  published_at, updated_at, authors_json, categories_json,
                  summary, relevance_et, impact_level, sectors_json, cited_by_count,
                  inserted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r.source,
                    r.external_id,
                    r.url,
                    r.title,
                    r.abstract,
                    r.published_at,
                    r.updated_at,
                    r.authors_json,
                    r.categories_json,
                    r.summary,
                    r.relevance_et,
                    r.impact_level,
                    r.sectors_json,
                    r.cited_by_count,
                    utc_now_iso(),
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    return inserted, skipped


def list_papers(con: sqlite3.Connection, *, limit: int = 10) -> list[sqlite3.Row]:
    cur = con.execute(
        """
        SELECT id, source, external_id, title, published_at, url, summary,
               relevance_et, impact_level, sectors_json
        FROM papers
        ORDER BY datetime(inserted_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    return list(cur.fetchall())


def query_papers(
    con: sqlite3.Connection,
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sector: Optional[str] = None,
    impact: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Filter papers. Optional sector matches json_extract on sectors_json array."""
    clauses: list[str] = ["1=1"]
    params: list[Any] = []

    # Compare calendar dates using ISO prefix YYYY-MM-DD
    if date_from:
        clauses.append("substr(COALESCE(published_at, inserted_at), 1, 10) >= ?")
        params.append(date_from[:10])
    if date_to:
        clauses.append("substr(COALESCE(published_at, inserted_at), 1, 10) <= ?")
        params.append(date_to[:10])
    if impact:
        clauses.append("LOWER(COALESCE(impact_level,'')) = LOWER(?)")
        params.append(impact)
    if source:
        clauses.append("LOWER(source) = LOWER(?)")
        params.append(source)
    if search:
        like = f"%{search.lower()}%"
        clauses.append(
            "(LOWER(title) LIKE ? OR LOWER(COALESCE(abstract,'')) LIKE ? OR LOWER(COALESCE(summary,'')) LIKE ?)"
        )
        params.extend([like, like, like])
    if sector:
        # sectors_json contains "sector":"Name" — LIKE works without JSON1
        clauses.append("LOWER(COALESCE(sectors_json,'')) LIKE ?")
        params.append(f"%{sector.lower()}%")

    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT id, source, external_id, url, title, abstract, published_at, summary,
               relevance_et, impact_level, sectors_json, cited_by_count, categories_json
        FROM papers
        WHERE {where_sql}
        ORDER BY datetime(COALESCE(published_at, inserted_at)) DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def count_papers(con: sqlite3.Connection) -> int:
    row = con.execute("SELECT COUNT(*) AS c FROM papers").fetchone()
    return int(row["c"]) if row else 0


def get_paper_by_id(con: sqlite3.Connection, paper_id: int) -> Optional[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT id, source, external_id, url, title, abstract, published_at, summary,
               relevance_et, impact_level, sectors_json, cited_by_count, authors_json, categories_json
        FROM papers WHERE id = ?
        """,
        (paper_id,),
    )
    r = cur.fetchone()
    return dict(r) if r else None


def papers_for_trends(con: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        con.execute(
            """
            SELECT id, published_at, sectors_json, title, abstract, summary, inserted_at
            FROM papers
            WHERE published_at IS NOT NULL OR inserted_at IS NOT NULL
            """
        ).fetchall()
    )


def save_daily_insight(
    con: sqlite3.Connection,
    *,
    report_for_date: str,
    payload: dict[str, Any],
) -> None:
    con.execute(
        """
        INSERT INTO daily_insights(report_for_date, generated_at, payload_json)
        VALUES (?, ?, ?)
        ON CONFLICT(report_for_date) DO UPDATE SET
          generated_at = excluded.generated_at,
          payload_json = excluded.payload_json
        """,
        (report_for_date[:10], utc_now_iso(), json.dumps(payload, ensure_ascii=False)),
    )


def get_latest_daily_insight(con: sqlite3.Connection) -> Optional[dict[str, Any]]:
    row = con.execute(
        """
        SELECT report_for_date, generated_at, payload_json
        FROM daily_insights
        ORDER BY report_for_date DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    data = json.loads(row["payload_json"])
    data["report_for_date"] = row["report_for_date"]
    data["stored_generated_at"] = row["generated_at"]
    return data


def list_daily_insights(con: sqlite3.Connection, *, limit: int = 14) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT report_for_date, generated_at, payload_json
        FROM daily_insights
        ORDER BY report_for_date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        payload = json.loads(r["payload_json"])
        payload["report_for_date"] = r["report_for_date"]
        payload["stored_generated_at"] = r["generated_at"]
        out.append(payload)
    return out
