-- dry-run checks (rolled back): new columns, one capture + one event pass in session
select count(*) filter (where ref_price is not null) ref_set, count(*) from public.intraday_context
 where for_date = to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD');
select public.intraday_capture() > -1 as capture_ok;
select code, prev_close, ref_price, limit_up, limit_down, long_term, allow_add from public.intraday_context
 where for_date = to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD') order by (ref_price is distinct from prev_close) desc limit 8;
select public.intraday_events() as events;
select code, left(body, 140) from public.intraday_outbox
 where for_date = to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD') order by id desc limit 8;
