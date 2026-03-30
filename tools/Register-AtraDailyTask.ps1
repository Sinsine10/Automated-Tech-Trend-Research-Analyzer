# Register a Windows Scheduled Task to run ATRA every day at 06:00 (local time).
# Run PowerShell AS ADMINISTRATOR once, from any directory:
#   powershell -ExecutionPolicy Bypass -File "C:\Users\Admin\mint\tools\Register-AtraDailyTask.ps1"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$DailyScript = Join-Path $Root "daily.ps1"

if (-not (Test-Path $DailyScript)) {
    Write-Error "daily.ps1 not found at $DailyScript"
    exit 1
}

$TaskName = "ATRA Daily Intelligence"
$Arg = "-NoProfile -ExecutionPolicy Bypass -File `"$DailyScript`""

$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument $Arg
$Trigger = New-ScheduledTaskTrigger -Daily -At "6:00AM"

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Description "MInT ATRA: ingest research, trends, daily briefing" `
    -RunLevel Highest

Write-Host "Registered task: $TaskName (daily 06:00) -> $DailyScript" -ForegroundColor Green
