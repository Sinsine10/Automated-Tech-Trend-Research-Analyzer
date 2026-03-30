# ATRA — start the full product (data refresh + API + dashboard)
# Usage: right-click → Run with PowerShell, or:  powershell -ExecutionPolicy Bypass -File .\run.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

Write-Host "=== ATRA launcher ===" -ForegroundColor Cyan

# Optional venv
if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) {
    Write-Host "Creating .venv ..." -ForegroundColor Yellow
    python -m venv .venv
}
& ".\.venv\Scripts\Activate.ps1"

python -m pip install -U pip -q
pip install -e . -q

if (-not $env:ATRA_CONTACT_EMAIL) {
    $env:ATRA_CONTACT_EMAIL = "atra@mint.gov.et"
    Write-Host "Set ATRA_CONTACT_EMAIL=$env:ATRA_CONTACT_EMAIL (change if you like)" -ForegroundColor DarkGray
}

Write-Host "Initializing DB + daily intelligence (ingest, NLP, tags, briefing) ..." -ForegroundColor Yellow
python -m atra init-db
python -m atra daily --days 1 --arxiv-limit 12 --openalex-limit 25

Write-Host ""
Write-Host "Starting services in new windows:" -ForegroundColor Green
# Port 8000 is often reserved/blocked on Windows; 8800 avoids WinError 10013
$ApiPort = if ($env:ATRA_API_PORT) { $env:ATRA_API_PORT } else { "8800" }
$UiPort  = if ($env:ATRA_UI_PORT) { $env:ATRA_UI_PORT } else { "8501" }
Write-Host "  API:        http://127.0.0.1:$ApiPort   (docs: /docs)" -ForegroundColor White
Write-Host "  Dashboard:  http://127.0.0.1:$UiPort" -ForegroundColor White
Write-Host ""

$apiCmd = "Set-Location '$Root'; & '.\.venv\Scripts\Activate.ps1'; uvicorn atra.api.main:app --host 127.0.0.1 --port $ApiPort"
$uiCmd  = "Set-Location '$Root'; & '.\.venv\Scripts\Activate.ps1'; streamlit run `"$Root\src\atra\dashboard\app.py`" --server.address 127.0.0.1 --server.port $UiPort"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $apiCmd
Start-Sleep -Seconds 2
Start-Process powershell -ArgumentList "-NoExit", "-Command", $uiCmd

Write-Host "Done. Close those windows to stop the servers." -ForegroundColor Cyan
