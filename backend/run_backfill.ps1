# 強制 UTF-8，避免中文亂碼
[Console]::InputEncoding  = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding           = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null

Remove-Item Env:\DATABASE_URL -ErrorAction SilentlyContinue
Set-Location "D:\個人資料\投資理財\股票網站分析\Stock-analysis-\backend"

$log = "backfill_log.txt"
"" | Out-File $log -Encoding UTF8

"=== K線回補開始 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Tee-Object -FilePath $log -Append
python -X utf8 -u local_backfill.py --market all --missing-only --no-daily 2>&1 | Tee-Object -FilePath $log -Append
"=== K線回補完成 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Tee-Object -FilePath $log -Append

"=== 籌碼+基本面開始 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Tee-Object -FilePath $log -Append
python -X utf8 -u local_chip_backfill.py --months 12 2>&1 | Tee-Object -FilePath $log -Append
"=== 全部完成 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Tee-Object -FilePath $log -Append
