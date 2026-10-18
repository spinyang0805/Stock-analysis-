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
"[$(Get-Date -Format s)] start" | Out-File -Append -Encoding utf8 $log
try {
    gh release download daily-picks -p latest.json -D $dir --clobber
    $d = (Get-Content (Join-Path $dir "latest.json") -Raw | ConvertFrom-Json).as_of
    gh release download daily-picks -p "report_$d.md" -D $dir --clobber
    python backend/tuning/ai_commentary.py --mode cli --as-of $d --dir $dir *>> $log
    if ($LASTEXITCODE -ne 0) { throw "ai_commentary failed ($LASTEXITCODE)" }
    gh release upload daily-picks (Join-Path $dir "ai_$d.md") --clobber
    "[$(Get-Date -Format s)] uploaded ai_$d.md" | Out-File -Append -Encoding utf8 $log
} catch {
    "[$(Get-Date -Format s)] ERROR $_" | Out-File -Append -Encoding utf8 $log
    exit 1
}
