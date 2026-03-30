from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from atra.db import DEFAULT_DB_PATH, connect, init_db, insert_run, list_papers, upsert_papers
from atra.sources.arxiv import ArxivIngestParams, fetch_arxiv
from atra.sources.openalex import OpenAlexParams, fetch_openalex
from atra.summarize import summarize_missing
from atra.tagging import tag_missing_papers
from atra.insights import compute_daily_insights, generate_and_store_daily_insight
from atra.trends import early_signals, sector_trend_series, top_tokens

app = typer.Typer(add_completion=False, help="ATRA — Automated Tech-Trend & Research Analyzer")
console = Console()

ARXIV_DEFAULT_CATEGORIES = ["cs.AI", "cs.LG", "cs.CV", "q-bio.BM", "astro-ph.IM"]


def _ingest_arxiv(
    db_path: Path,
    *,
    category: str,
    days: int,
    limit: int,
) -> tuple[int, int, int, int]:
    rows, params_json = fetch_arxiv(ArxivIngestParams(category=category, days=days, limit=limit))
    con = connect(db_path)
    try:
        run_id = insert_run(con, source="arxiv", params_json=params_json)
        inserted, skipped = upsert_papers(con, rows)
        con.commit()
    finally:
        con.close()
    return run_id, len(rows), inserted, skipped


def _ingest_openalex(
    db_path: Path,
    *,
    days: int,
    limit: int,
    search: Optional[str],
) -> tuple[int, int, int, int]:
    rows, params_json = fetch_openalex(OpenAlexParams(days=days, limit=min(limit, 200), search=search))
    con = connect(db_path)
    try:
        run_id = insert_run(con, source="openalex", params_json=params_json)
        inserted, skipped = upsert_papers(con, rows)
        con.commit()
    finally:
        con.close()
    return run_id, len(rows), inserted, skipped


@app.command("init-db")
def init_db_cmd(db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db", help="SQLite DB path")) -> None:
    init_db(db_path)
    console.print(f"[green]OK[/green] Initialized DB at {db_path}")


@app.command("ingest")
def ingest_cmd(
    source: str = typer.Argument(..., help="arxiv | openalex"),
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    category: str = typer.Option("cs.AI", "--category", help="arXiv category"),
    days: int = typer.Option(7, "--days", help="Last N days"),
    limit: int = typer.Option(50, "--limit", help="Max items"),
    search: Optional[str] = typer.Option(None, "--search", help="OpenAlex keyword search"),
) -> None:
    source = source.lower().strip()
    init_db(db_path)
    if source == "arxiv":
        run_id, n, ins, sk = _ingest_arxiv(db_path, category=category, days=days, limit=limit)
    elif source == "openalex":
        run_id, n, ins, sk = _ingest_openalex(db_path, days=days, limit=limit, search=search)
    else:
        raise typer.BadParameter("Use source: arxiv or openalex")
    console.print(f"[green]OK[/green] run_id={run_id} fetched={n} inserted={ins} skipped={sk}")


@app.command("ingest-all")
def ingest_all_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    days: int = typer.Option(7, "--days"),
    arxiv_limit: int = typer.Option(40, "--arxiv-limit", help="Per arXiv category"),
    openalex_limit: int = typer.Option(80, "--openalex-limit"),
    openalex_search: Optional[str] = typer.Option(None, "--openalex-search"),
) -> None:
    """Ingest several arXiv categories + OpenAlex."""
    init_db(db_path)
    for cat in ARXIV_DEFAULT_CATEGORIES:
        rid, n, ins, sk = _ingest_arxiv(db_path, category=cat, days=days, limit=arxiv_limit)
        console.print(f"  arxiv {cat}: run {rid} fetched={n} +{ins} skip {sk}")
    rid, n, ins, sk = _ingest_openalex(
        db_path, days=days, limit=openalex_limit, search=openalex_search
    )
    console.print(f"  openalex: run {rid} fetched={n} +{ins} skip {sk}")
    console.print("[green]OK[/green] ingest-all finished")


@app.command("summarize")
def summarize_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    batch_limit: int = typer.Option(500, "--batch-limit"),
) -> None:
    n = summarize_missing(db_path, batch_limit=batch_limit)
    console.print(f"[green]OK[/green] summarized {n} papers")


@app.command("tag")
def tag_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    batch_limit: int = typer.Option(1000, "--batch-limit"),
) -> None:
    n = tag_missing_papers(db_path, batch_limit=batch_limit)
    console.print(f"[green]OK[/green] tagged {n} papers")


@app.command("pipeline")
def pipeline_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    days: int = typer.Option(7, "--days"),
    arxiv_limit: int = typer.Option(35, "--arxiv-limit"),
    openalex_limit: int = typer.Option(60, "--openalex-limit"),
    openalex_search: Optional[str] = typer.Option(None, "--openalex-search"),
) -> None:
    """Ingest → summarize → tag (daily-style)."""
    init_db(db_path)
    for cat in ARXIV_DEFAULT_CATEGORIES:
        rid, n, ins, sk = _ingest_arxiv(db_path, category=cat, days=days, limit=arxiv_limit)
        console.print(f"  arxiv {cat}: run {rid} +{ins} skip {sk}")
    rid, n, ins, sk = _ingest_openalex(
        db_path, days=days, limit=openalex_limit, search=openalex_search
    )
    console.print(f"  openalex: run {rid} +{ins} skip {sk}")

    s = summarize_missing(db_path, batch_limit=2000)
    t = tag_missing_papers(db_path, batch_limit=2000)
    console.print(f"[green]OK[/green] pipeline: summarized {s}, tagged {t}")


@app.command("insights")
def insights_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    store: bool = typer.Option(True, "--store/--no-store", help="Persist to daily_insights table"),
) -> None:
    """Compute trend spikes, emerging keywords, and executive briefing (no ingest)."""
    if store:
        payload = generate_and_store_daily_insight(db_path)
    else:
        payload = compute_daily_insights(db_path)
    console.print("[bold]Briefing[/bold]")
    for b in payload.get("narrative_bullets", []):
        console.print(f"  - {b}")
    slim = {
        "headline_stats": payload.get("headline_stats"),
        "sector_momentum": (payload.get("sector_momentum") or [])[:10],
        "emerging_keywords": (payload.get("emerging_keywords") or [])[:12],
        "priority_brief": (payload.get("priority_brief") or [])[:5],
    }
    console.print_json(data=slim)


@app.command("daily")
def daily_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    days: int = typer.Option(1, "--days", help="Ingest window (last N days)"),
    arxiv_limit: int = typer.Option(25, "--arxiv-limit"),
    openalex_limit: int = typer.Option(50, "--openalex-limit"),
    openalex_search: Optional[str] = typer.Option(None, "--openalex-search"),
    skip_insights: bool = typer.Option(False, "--skip-insights", help="Ingest only, no briefing"),
) -> None:
    """Automated daily job: full ingest → summarize → tag → intelligence briefing."""
    init_db(db_path)
    for cat in ARXIV_DEFAULT_CATEGORIES:
        rid, n, ins, sk = _ingest_arxiv(db_path, category=cat, days=days, limit=arxiv_limit)
        console.print(f"  arxiv {cat}: run {rid} +{ins} skip {sk}")
    rid, n, ins, sk = _ingest_openalex(
        db_path, days=days, limit=openalex_limit, search=openalex_search
    )
    console.print(f"  openalex: run {rid} +{ins} skip {sk}")
    s = summarize_missing(db_path, batch_limit=5000)
    t = tag_missing_papers(db_path, batch_limit=5000)
    console.print(f"  summarized {s}, tagged {t}")
    if not skip_insights:
        payload = generate_and_store_daily_insight(db_path)
        console.print("[green]OK[/green] daily intelligence briefing stored")
        for b in payload.get("narrative_bullets", []):
            console.print(f"  - {b}")
    else:
        console.print("[green]OK[/green] daily ingest finished (insights skipped)")


@app.command("list")
def list_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    limit: int = typer.Option(10, "--limit"),
) -> None:
    init_db(db_path)
    con = connect(db_path)
    try:
        rows = list_papers(con, limit=limit)
    finally:
        con.close()

    table = Table(title=f"ATRA papers (latest {min(limit, len(rows))})")
    table.add_column("source", style="cyan")
    table.add_column("published_at", style="magenta")
    table.add_column("impact", style="yellow")
    table.add_column("ET rel.", style="green")
    table.add_column("title", overflow="fold")
    table.add_column("summary", overflow="fold", max_width=40)

    for r in rows:
        summ = (r["summary"] or "")[:160] + ("…" if r["summary"] and len(str(r["summary"])) > 160 else "")
        table.add_row(
            str(r["source"]),
            str(r["published_at"] or "")[:10],
            str(r["impact_level"] or "—"),
            str(r["relevance_et"] if r["relevance_et"] is not None else "—"),
            str(r["title"] or ""),
            summ or "—",
        )

    console.print(table)


@app.command("trends")
def trends_cmd(
    db_path: Path = typer.Option(DEFAULT_DB_PATH, "--db"),
    keywords_top: int = typer.Option(20, "--keywords-top"),
) -> None:
    series = sector_trend_series(db_path)
    kws = top_tokens(db_path, top_n=keywords_top)
    sig = early_signals(db_path, recent_days=14)
    payload = {"sector_series_sample": series[:60], "keywords": kws, "signals": sig[:15]}
    console.print_json(data=payload)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
