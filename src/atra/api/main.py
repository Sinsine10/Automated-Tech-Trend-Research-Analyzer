from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from atra.db import (
    connect,
    count_papers,
    get_latest_daily_insight,
    get_paper_by_id,
    init_db,
    list_daily_insights,
    query_papers,
)
from atra.tagging import list_sector_names
from atra.trends import early_signals, sector_trend_series, top_tokens


def _db_path() -> Path:
    raw = os.environ.get("ATRA_DB_PATH", "data/atra.db")
    return Path(raw)


def create_app() -> FastAPI:
    app = FastAPI(
        title="ATRA API",
        description="Automated Tech-Trend & Research Analyzer — MInT",
        version="1.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in os.environ.get("ATRA_CORS_ORIGINS", "*").split(",") if o.strip()],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def _startup() -> None:
        init_db(_db_path())

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/meta/sectors")
    def meta_sectors() -> dict[str, list[str]]:
        return {"sectors": list_sector_names()}

    @app.get("/meta/stats")
    def meta_stats() -> dict[str, Any]:
        con = connect(_db_path())
        try:
            n = count_papers(con)
        finally:
            con.close()
        return {"papers": n, "db": str(_db_path())}

    @app.get("/papers")
    def papers(
        date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
        date_to: Optional[str] = Query(None),
        sector: Optional[str] = None,
        impact: Optional[str] = Query(None, description="low | medium | high"),
        source: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        imp = impact.strip().lower() if impact else None
        con = connect(_db_path())
        try:
            rows = query_papers(
                con,
                date_from=date_from,
                date_to=date_to,
                sector=sector,
                impact=imp,
                source=source,
                search=search,
                limit=limit,
                offset=offset,
            )
        finally:
            con.close()
        return {"items": rows, "limit": limit, "offset": offset}

    @app.get("/papers/{paper_id}")
    def paper_one(paper_id: int) -> dict[str, Any]:
        con = connect(_db_path())
        try:
            row = get_paper_by_id(con, paper_id)
        finally:
            con.close()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")
        return row

    @app.get("/trends/sectors")
    def trends_sectors() -> dict[str, Any]:
        return {"series": sector_trend_series(_db_path())}

    @app.get("/trends/keywords")
    def trends_keywords(top_n: int = Query(30, ge=5, le=100)) -> dict[str, Any]:
        return {"keywords": top_tokens(_db_path(), top_n=top_n)}

    @app.get("/signals/recent")
    def signals_recent(days: int = Query(14, ge=1, le=90)) -> dict[str, Any]:
        return {"signals": early_signals(_db_path(), recent_days=days)}

    @app.get("/insights/latest")
    def insights_latest() -> dict[str, Any]:
        con = connect(_db_path())
        try:
            row = get_latest_daily_insight(con)
        finally:
            con.close()
        if not row:
            raise HTTPException(
                status_code=404,
                detail="No daily briefing yet. Run: python -m atra daily",
            )
        return row

    @app.get("/insights/history")
    def insights_history(limit: int = Query(14, ge=1, le=90)) -> dict[str, Any]:
        con = connect(_db_path())
        try:
            items = list_daily_insights(con, limit=limit)
        finally:
            con.close()
        return {"items": items, "limit": limit}

    return app


app = create_app()
