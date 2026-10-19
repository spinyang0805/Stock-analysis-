# ai_morning.ps1 — runs on the user's PC (Windows Task Scheduler, trading days ~08:05).
# Downloads last evening's report from release daily-picks, writes the 3-model commentary
# (claude -p / codex exec / gemini -p, merged by claude) and uploads ai_<date>.md before the 08:50 send.
# If this does not run (PC off), daily-send.yml falls back to Groq automatically.
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$dir = Join-Path $repo "history\out\daily"
New-Item -ItemType Directory -Force $dir | Out-Null
$log = Join-Path $dir "ai_morning.log"

function Log($m) { "[$(Get-Date -Format s)] $m" | Out-File -Append -Encoding utf8 $log }
function Invoke-GhChecked {
    # (named differently from gh: PowerShell names are case-insensitive, 'Gh' would call itself)
    # native commands do not throw in Windows PowerShell 5.1 → check the exit code after every gh call
    & gh @args
    if ($LASTEXITCODE -ne 0) { throw "gh $($args -join ' ') failed ($LASTEXITCODE)" }
}

Log "start"
try {
    Remove-Item (Join-Path $dir "latest.json") -ErrorAction SilentlyContinue   # never reuse a cached pointer
    Invoke-GhChecked release download daily-picks -p latest.json -D $dir --clobber
    $d = (Get-Content (Join-Path $dir "latest.json") -Raw | ConvertFrom-Json).as_of
    Remove-Item (Join-Path $dir "report_$d.md") -ErrorAction SilentlyContinue
    Invoke-GhChecked release download daily-picks -p "report_$d.md" -D $dir --clobber
    python backend/tuning/ai_commentary.py --mode cli --as-of $d --dir $dir *>> $log
    if ($LASTEXITCODE -ne 0) { throw "ai_commentary failed ($LASTEXITCODE)" }
    Invoke-GhChecked release upload daily-picks (Join-Path $dir "ai_$d.md") --clobber
    Log "uploaded ai_$d.md"
} catch {
    Log "ERROR $_"
    exit 1
}
