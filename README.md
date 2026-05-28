# Automated Tech Trend & Research Analyzer (ATRA)

## Overview

ATRA (Automated Tech Trend & Research Analyzer) is an AI-powered research intelligence platform designed to automatically collect, analyze, summarize, and visualize emerging technology research trends.

The system gathers academic papers from public research sources, processes them using Natural Language Processing (NLP) techniques, stores them in a database, and provides interactive dashboards and API access for insights and trend monitoring.

The project combines:

* AI/NLP-based paper summarization
* Automated research ingestion pipelines
* Trend analysis
* FastAPI backend services
* Streamlit visualization dashboards
* SQLite database storage


---

# Tech Stack

## Backend

* Python
* FastAPI
* Uvicorn

## AI / Machine Learning

* Transformers
* Sentence Transformers
* Scikit-learn
* NLP-based summarization

## Data Processing

* Pandas
* Requests
* BeautifulSoup
* Feedparser

## Database

* SQLite
* SQLAlchemy

## Frontend / Visualization

* Streamlit


---

# Installation

## Clone Repository

```bash
git clone https://github.com/Sinsine10/Automated-Tech-Trend-Research-Analyzer
cd Automated-Tech-Trend-Research-Analyzer
```

---

## Create Virtual Environment

### Windows

```bash
python -m venv .venv
.venv\Scripts\activate
```

### Linux / Mac

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Running the Project

## Step 1 — Run Daily Research Pipeline

```bash
python -m atra daily --days 1 --arxiv-limit 15 --openalex-limit 30
```


## Step 2 — Start FastAPI Backend

```bash
uvicorn atra.api.main:app --host 127.0.0.1 --port 8800
```

## Step 3 — Start Streamlit Dashboard

```bash
streamlit run src\atra\dashboard\app.py --server.address 127.0.0.1 --server.port 8501
```

Dashboard:

```text
http://127.0.0.1:8501
```

---

# AI & NLP Components

The platform uses multiple AI and NLP techniques including:

* Research paper summarization
* Semantic text analysis
* Keyword extraction
* Topic clustering
* Trend detection
* Similarity analysis
* Research intelligence generation


The project currently uses SQLite for lightweight local storage.

Database stores:

* Research papers
* Metadata
* Summaries
* Keywords
* Trend analytics
* Pipeline execution logs

---

# API Endpoints

Example endpoints:

```text
GET /papers
GET /trends
GET /summary/{id}
GET /health
```

Interactive Swagger documentation available at:

```text
http://127.0.0.1:8800/docs
```


---

# License

This project is for educational and research purposes.

