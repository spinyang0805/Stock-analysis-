-- read-only check (run by db-validate inside a rolled-back transaction)
select jobname, schedule from cron.job where jobname like 'intraday%' order by 1;
select code, kind, price, created_at at time zone 'Asia/Taipei' as tw, result from public.alert_log where for_date = to_char(now() at time zone 'Asia/Taipei','YYYYMMDD') order by created_at;
select count(*) as ctx_rows, count(*) filter (where role='holding') as holding from public.intraday_context where for_date = to_char(now() at time zone 'Asia/Taipei','YYYYMMDD');
select code, created_at at time zone 'Asia/Taipei' as tw, urgent, teams_sent, line_sent, left(body, 80) from public.intraday_outbox order by id desc limit 10;