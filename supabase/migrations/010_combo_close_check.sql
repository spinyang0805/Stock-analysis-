-- 組合拳 13:20 收盤前檢查（使用者 2026-10-20 要求）：
--   盤中突破約一半會在收盤跌回（回測：站穩者 20 天 +6.1%、跌回者 +0.9%），所以 13:20 再看一次
--   「組合拳待突破」名單：仍站在突破價（前 20 日高）之上 → 提醒「收盤前可考慮買進」；
--   盤中曾突破但已跌回 → 標示「假突破，不買」。Teams 一定送；有站穩的才推 LINE（計入每月 150 次上限）。
--   每個交易日只送一次（combo_close_log）；沒有 intraday_context 的日子（非交易日）直接返回。

create table if not exists public.combo_close_log (
  for_date  text primary key,
  sent_at   timestamptz not null default now(),
  body      text,
  result    jsonb
);
alter table public.combo_close_log enable row level security;

create or replace function public.combo_close_check()
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare
  tw timestamp := now() at time zone 'Asia/Taipei';
  d text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD');
  v_ym text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMM');
  day0 timestamptz := (tw::date::timestamp + time '09:00') at time zone 'Asia/Taipei';
  above text; failed text; n_above int := 0; body text; st_teams int; st_line int; used int;
begin
  if exists (select 1 from combo_close_log where for_date = d) then return 0; end if;
  if not exists (select 1 from intraday_context where for_date = d and role = 'pick' and name like '%組合拳%') then
    return 0;
  end if;
  with w as (
    select c.code, replace(c.name, '（組合拳待突破）', '') as name, c.hh20
      from intraday_context c
     where c.for_date = d and c.role = 'pick' and c.name like '%組合拳%' and c.hh20 is not null
  ), px as (
    select w.*,
           (select b.close from intraday_bars b where b.code = w.code and b.minute >= now() - interval '10 minutes'
             order by b.minute desc limit 1) as now_px,
           (select max(b.high) from intraday_bars b where b.code = w.code and b.minute >= day0) as day_high
      from w
  )
  select string_agg(format('🟢 %s %s 現價 %s（突破價 %s，+%s%%）', code, name, now_px, hh20,
                           round((now_px / hh20 - 1) * 100, 1)), E'\n' order by now_px / hh20 desc)
           filter (where now_px > hh20),
         count(*) filter (where now_px > hh20),
         string_agg(format('⚪ %s %s 盤中曾到 %s，現價 %s 已跌回突破價 %s 之下（假突破，不買）', code, name,
                           day_high, now_px, hh20), E'\n' order by code)
           filter (where day_high > hh20 and now_px <= hh20)
    into above, n_above, failed
    from px;

  body := '⏰ 13:20 組合拳收盤前檢查' || E'\n'
          || coalesce('仍站穩突破價，可考慮收盤前買進：' || E'\n' || above, '目前沒有站穩突破價的觀察股。')
          || coalesce(E'\n' || failed, '')
          || E'\n' || '收盤價站在突破價之上才是組合拳買點；盤中買進要承擔尾盤拉回的風險。';

  st_teams := _post_json(_vault('teams_webhook_url'), jsonb_build_object('type', 'message', 'attachments', jsonb_build_array(
    jsonb_build_object('contentType', 'application/vnd.microsoft.card.adaptive', 'content', jsonb_build_object(
      '$schema', 'http://adaptivecards.io/schemas/adaptive-card.json', 'type', 'AdaptiveCard', 'version', '1.4',
      'body', jsonb_build_array(jsonb_build_object('type', 'TextBlock', 'text', body, 'wrap', true)))))));

  if n_above > 0 and _vault('line_channel_token') is not null then
    insert into alert_quota (ym, line_sent) values (v_ym, 0) on conflict do nothing;
    select line_sent into used from alert_quota where alert_quota.ym = v_ym for update;
    if used < 150 then
      st_line := _post_json('https://api.line.me/v2/bot/message/push',
        jsonb_build_object('to', _vault('line_user_id'), 'messages', jsonb_build_array(jsonb_build_object(
          'type', 'text', 'text', '⏰ 13:20 組合拳仍站穩突破價' || E'\n' || above || E'\n' || '收盤站穩才確認'))),
        'Bearer ' || _vault('line_channel_token'));
      if st_line = 200 then
        update alert_quota set line_sent = line_sent + 1 where alert_quota.ym = v_ym;
      end if;
    end if;
  end if;

  insert into combo_close_log (for_date, body, result)
  values (d, body, jsonb_build_object('teams', st_teams, 'line', st_line, 'above', n_above));
  return n_above;
end $$;
revoke all on function public.combo_close_check() from public, anon, authenticated;

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname = 'combo-close-check';
  -- pg_cron runs in UTC: 05:20 UTC = 13:20 Taipei, Mon–Sat (non-trading days have no context → no-op)
  perform cron.schedule('combo-close-check', '20 5 * * 1-6', 'select public.combo_close_check()');
end $$;
