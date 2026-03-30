# ATRA — scheduled daily intelligence (ingest + summarize + tag + briefing)
# Point Windows Task Scheduler at this file (see tools\Register-AtraDailyTask.ps1).

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

if (Test-Path ".\.venv\Scripts\Activate.ps1") {
    & ".\.venv\Scripts\Activate.ps1"
}

if (-not $env:ATRA_CONTACT_EMAIL) {
    $env:ATRA_CONTACT_EMAIL = "atra@mint.gov.et"
}

# Tune for production: raise limits after validating API politeness / disk use
python -m atra daily --days 1 --arxiv-limit 20 --openalex-limit 40
