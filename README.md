# ATRA — Automated Tech-Trend & Research Analyzer

**Organization:** Ministry of Innovation and Technology (MInT) · **Department:** AI interns  

End-to-end prototype: **multi-source ingestion**, **3-sentence summaries**, **Ethiopia-oriented sector tagging & impact**, **trends**, **REST API**, and **Streamlit dashboard**.

## Features

| Area | What you get |
|------|----------------|
| **Data** | arXiv (AI/ML/CV, bio, space) + **OpenAlex** recent works (metadata; abstracts when available) |
| **NLP** | Executive **summary** (first 3 sentences of abstract — swap for a transformer later) |
| **National lens** | **Sectors** + **Ethiopia relevance** score + **impact** (low/medium/high) |
| **Trends** | Daily counts by sector, keyword frequencies, “early signal” list |
| **Daily intelligence** | **Automated briefing**: sector momentum vs 7-day baseline, emerging keywords, priority items → stored in `daily_insights` |
| **API** | **FastAPI** (`/papers`, `/trends/*`, `/signals/recent`, `/insights/*`, `/meta/*`) |
| **UI** | **Streamlit** dashboard: **Daily briefing** tab + papers / trends / signals |

## Setup (Windows PowerShell)

```powershell
cd c:\Users\Admin\mint
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e .
```

**OpenAlex polite use:** set your contact email (recommended):

```powershell
$env:ATRA_CONTACT_EMAIL = "you@your-institution.gov.et"
```

## CLI

```powershell
python -m atra init-db

# One source
python -m atra ingest arxiv --category cs.AI --days 7 --limit 40
python -m atra ingest openalex --days 7 --limit 80 --search "machine learning"

# Several categories + OpenAlex
python -m atra ingest-all --days 7 --arxiv-limit 30 --openalex-limit 60

# NLP + tagging
python -m atra summarize --batch-limit 500
python -m atra tag --batch-limit 1000

# Full ingest → summarize → tag (no stored briefing)
python -m atra pipeline --days 7 --arxiv-limit 25 --openalex-limit 50

# Recommended “production” cadence: ingest + NLP + stored daily briefing
python -m atra daily --days 1 --arxiv-limit 20 --openalex-limit 40

# Recompute briefing only (after ingest)
python -m atra insights

python -m atra list --limit 15
python -m atra trends --keywords-top 20
```

Database file: **`data/atra.db`** (delete the `data` folder to reset).  
Override path: `$env:ATRA_DB_PATH = "D:\data\atra.db"`.

## REST API

```powershell
# Default 8000; on some Windows setups port 8000 is blocked — use 8800:
uvicorn atra.api.main:app --reload --host 127.0.0.1 --port 8800
```

**One-click (venv + pipeline + API + dashboard):** `powershell -ExecutionPolicy Bypass -File .\run.ps1`

- `GET /health`
- `GET /meta/sectors` — sector taxonomy used for tagging  
- `GET /meta/stats`
- `GET /papers?date_from=2026-01-01&sector=Agriculture&impact=high&search=...`
- `GET /papers/{id}`
- `GET /trends/sectors`
- `GET /trends/keywords?top_n=30`
- `GET /signals/recent?days=14`
- `GET /insights/latest` — today’s stored briefing (JSON)
- `GET /insights/history?limit=14` — recent briefings

## Streamlit dashboard

From the project root (with `pip install -e .`):

```powershell
streamlit run src\atra\dashboard\app.py
```

## Scheduling (daily automation)

**Option A — helper script** (ingest + summarize + tag + briefing):

```powershell
# Edit limits inside if needed, then:
powershell -ExecutionPolicy Bypass -File .\daily.ps1
```

**Option B — register Windows Task Scheduler** (runs `daily.ps1` every day at 06:00):

```powershell
# Run PowerShell as Administrator once:
powershell -ExecutionPolicy Bypass -File .\tools\Register-AtraDailyTask.ps1
```

**Option C — manual command**

```powershell
cd c:\Users\Admin\mint
.\.venv\Scripts\Activate.ps1
python -m atra daily --days 1 --arxiv-limit 20 --openalex-limit 40
```

Tune limits for API politeness and disk use. The dashboard caches the latest briefing for ~2 minutes; for *true* sub-second live streams you would add WebSockets + a message queue (future hardening).

## Next improvements (more “advanced”)

- **Embeddings + clustering** for topic drift and duplicate detection across arXiv/OpenAlex.  
- **Anomaly detection** (e.g. z-scores on sector counts) instead of ratio heuristics.  
- Replace heuristic summary with a **local or API transformer** (abstractive).  
- Train a **relevance classifier** on labeled Ethiopian-policy examples.  
- **PMC / Semantic Scholar**, **email/Teams webhooks** for alerts.  
- **Auth** on API + dashboard for production.

## License

Internal / governmental use — confirm with MInT legal before public release.
