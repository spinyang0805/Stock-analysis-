-- 暫時降頻：盤中提醒改為每 10 分鐘批次一次（避免開盤後已跌破的價位一檔一檔逐分鐘發送）。
-- 之後由 009 的事件式提醒（確認、合併、冷卻、價格建議）取代。
do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname = 'intraday-alerts';
  perform cron.schedule('intraday-alerts', '*/10 * * * *', 'select public.intraday_alerts()');
end $$;
