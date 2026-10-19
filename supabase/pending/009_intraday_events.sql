-- 盤中「事件式」提醒（取代 006 的逐價位提醒）。設計經 Codex 討論（2026-10-19）：
--   只發有意義的事件：帶量上漲、帶量下殺、跌破停損、跌破支撐（月線/季線/20日低）、突破 20 日高、
--   回測月線止跌、開盤跳空 ≥3%、接近漲跌停。條件須「連續 2 根已完成的 1 分 K」成立。
--   去重：同股同事件 30 分鐘冷卻且價格需再變動 ≥ max(1%, 0.5ATR)；同股 10 分鐘內只發一次；
--   每股每日最多 3 則、全日最多 20 則（緊急持股風險另可升級 2 次）；同股多事件合併為一則；
--   每 5 分鐘批次送 Teams，持股跌破停損等緊急事件立即送並推 LINE（每日 ≤5、每月 ≤150）。
--   每則附建議價：買進區間／停損／目標（追高、接近漲停、13:20 後不給買價）；賣出建議價。
-- 每晚 daily-picks（alert_levels.write_context）寫入隔天的 intraday_context；沒有 context 的日子
-- （非交易日）函式直接返回。

create table if not exists public.intraday_context (
  for_date   text not null,
  code       text not null,
  name       text,
  role       text not null,                -- holding / tracked / pick
  etf        boolean not null default false,
  prev_close numeric not null,
  ma20       numeric, ma60 numeric, ma20_up boolean,
  atr        numeric,
  avg_vol20  numeric,                      -- 20 日均量（張）
  hh20       numeric, ll20 numeric,        -- 前 20 日最高 / 最低（含今日收盤當天）
  stop       numeric,                      -- 持股移動停損 / 追蹤股停損 / 推薦股初始停損
  target     numeric,                      -- 參考目標價
  primary key (for_date, code)
);
alter table public.intraday_context enable row level security;

create table if not exists public.intraday_event_state (
  for_date     text not null,
  code         text not null,
  event        text not null,
  last_sent_at timestamptz,
  last_price   numeric,
  sent_count   int not null default 0,
  primary key (for_date, code, event)
);
alter table public.intraday_event_state enable row level security;

create table if not exists public.intraday_outbox (
  id          bigserial primary key,
  for_date    text not null,
  code        text not null,
  created_at  timestamptz not null default now(),
  urgent      boolean not null default false,
  line_ok     boolean not null default false,  -- eligible for LINE
  body        text not null,
  teams_sent  boolean not null default false,
  line_sent   boolean not null default false
);
alter table public.intraday_outbox enable row level security;

/* 台股升降單位 */
create or replace function public.tw_tick(p numeric, etf boolean default false)
returns numeric language sql immutable as $$
  select case when etf then (case when p < 50 then 0.01 else 0.05 end)
              when p < 10 then 0.01 when p < 50 then 0.05 when p < 100 then 0.1
              when p < 500 then 0.5 when p < 1000 then 1 else 5 end
$$;
create or replace function public.tw_round(p numeric, etf boolean, mode text)
returns numeric language sql immutable as $$
  select case when p is null or p <= 0 then null
              when mode = 'down' then floor(p / tw_tick(p, etf)) * tw_tick(p, etf)
              when mode = 'up' then ceil(p / tw_tick(p, etf)) * tw_tick(p, etf)
              else round(p / tw_tick(p, etf)) * tw_tick(p, etf) end
$$;

/* 盤中累計成交量的時間分布（暫定；之後可用實際資料校正） */
create or replace function public._vol_profile(t time)
returns numeric language sql immutable as $$
  select case
    when t <= time '09:00' then 0.0
    when t <= time '09:05' then 0.10 * extract(epoch from t - time '09:00') / 300
    when t <= time '09:30' then 0.10 + 0.15 * extract(epoch from t - time '09:05') / 1500
    when t <= time '10:30' then 0.25 + 0.25 * extract(epoch from t - time '09:30') / 3600
    when t <= time '12:00' then 0.50 + 0.20 * extract(epoch from t - time '10:30') / 5400
    when t <= time '13:25' then 0.70 + 0.15 * extract(epoch from t - time '12:00') / 5100
    else 1.0 end
$$;

create or replace function public.intraday_events()
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare
  tw     timestamp := now() at time zone 'Asia/Taipei';
  d      text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD');
  cur_m  timestamptz := date_trunc('minute', now());
  r      record;
  ev     text[];
  txt    text;
  urgent boolean;
  n_new  int := 0;
  day_total int;
  sent_stock int;
begin
  if tw::time < time '09:02' or tw::time > time '13:31' then return 0; end if;
  if not exists (select 1 from intraday_context where for_date = d) then return 0; end if;
  select count(*) into day_total from intraday_outbox where for_date = d and not urgent;

  for r in
    with b as (                                 -- completed minutes only (the current bar is still forming)
      select x.code, x.minute, x.open, x.high, x.low, x.close, x.cum_vol,
             greatest(x.cum_vol - coalesce(lag(x.cum_vol) over (partition by x.code order by x.minute), 0), 0) v
        from intraday_bars x
        join intraday_context c on c.code = x.code and c.for_date = d
       where x.minute >= (tw::date::timestamp at time zone 'Asia/Taipei') and x.minute < cur_m
    ), a as (
      select code,
             (array_agg(close order by minute desc))[1] c1, (array_agg(close order by minute desc))[2] c2,
             (array_agg(close order by minute desc))[6] c6,
             (array_agg(low order by minute desc))[1] l1, (array_agg(low order by minute desc))[2] l2,
             (array_agg(open order by minute))[1] o0, min(minute) m0, max(minute) m1, count(*) nmin,
             max(cum_vol) cv,
             sum(v) filter (where minute >= cur_m - interval '5 minutes') v5,
             sum(((high + low + close) / 3) * v) / nullif(sum(v), 0) vwap,
             (array_agg(close order by minute desc) filter (where minute < (tw::date::timestamp at time zone 'Asia/Taipei') + interval '9 hours 5 minutes'))[1] c_0905
        from b group by code
    )
    select c.*, a.*,
           greatest(2 * tw_tick(c.prev_close, c.etf), 0.002 * c.prev_close, 0.1 * coalesce(c.atr, 0)) bf,
           a.cv / nullif(c.avg_vol20 * _vol_profile(tw::time), 0) pace,
           a.v5 / nullif(c.avg_vol20 * (_vol_profile(tw::time) - _vol_profile((tw - interval '5 minutes')::time)), 0) burst
      from intraday_context c join a on a.code = c.code
     where c.for_date = d and a.m1 >= cur_m - interval '3 minutes' and a.nmin >= 2
  loop
    ev := array[]::text[];
    urgent := false;
    -- 跌破停損（持股 / 追蹤）：連 2 根收在停損 − 緩衝之下；跌深 0.5 ATR 立即視為緊急
    if r.role in ('holding', 'tracked') and r.stop is not null
       and ((r.c1 < r.stop - r.bf and r.c2 < r.stop - r.bf) or r.c1 < r.stop - greatest(r.bf, 0.5 * coalesce(r.atr, 0))) then
      ev := ev || 'stop'; urgent := r.role = 'holding';
    end if;
    -- 帶量下殺 / 帶量上漲（09:10 後才判斷量能）
    if tw::time >= time '09:10' and r.vwap is not null and r.c6 is not null then
      if r.c1 / r.prev_close - 1 <= -0.02 and r.c1 / r.c6 - 1 <= -0.008 and r.c1 <= r.vwap - r.bf
         and r.c2 <= r.vwap - r.bf and coalesce(r.pace, 0) >= 1.5 and coalesce(r.burst, 0) >= 2 then
        ev := ev || 'heavy_sell';
      end if;
      if r.c1 / r.prev_close - 1 >= 0.02 and r.c1 / r.c6 - 1 >= 0.008 and r.c1 >= r.vwap + r.bf
         and r.c2 >= r.vwap + r.bf and coalesce(r.pace, 0) >= 1.5 and coalesce(r.burst, 0) >= 2 then
        ev := ev || 'heavy_rally';
      end if;
    end if;
    -- 跌破支撐：月線、季線、20 日低（昨天還在之上 → 今天連 2 根收在之下）
    if (r.ma20 is not null and r.prev_close > r.ma20 and r.c1 < r.ma20 - r.bf and r.c2 < r.ma20 - r.bf)
       or (r.ma60 is not null and r.prev_close > r.ma60 and r.c1 < r.ma60 - r.bf and r.c2 < r.ma60 - r.bf)
       or (r.ll20 is not null and r.c1 < r.ll20 - r.bf and r.c2 < r.ll20 - r.bf) then
      ev := ev || 'support_break';
    end if;
    -- 突破 20 日高（推薦 / 追蹤）：連 2 根站上且在均價之上、帶量
    if r.role in ('pick', 'tracked') and r.hh20 is not null and r.c1 > r.hh20 + r.bf and r.c2 > r.hh20 + r.bf
       and r.c1 > coalesce(r.vwap, 0) and coalesce(r.pace, 0) >= 1.5 then
      ev := ev || 'breakout';
    end if;
    -- 回測月線止跌（推薦 / 追蹤）：月線上揚、今日低點觸及月線、連 2 根收回月線與均價之上
    if r.role in ('pick', 'tracked') and r.ma20_up and r.ma20 is not null
       and least(r.l1, r.l2) <= r.ma20 + r.bf and least(r.l1, r.l2) >= r.ma20 - r.bf
       and r.c1 > r.ma20 + r.bf and r.c2 > r.ma20 + r.bf and r.c1 > coalesce(r.vwap, 0) + r.bf then
      ev := ev || 'ma20_rebound';
    end if;
    -- 開盤跳空 ≥3%，且 09:05 時仍保有 75% 的缺口
    if tw::time >= time '09:05' and r.o0 is not null and abs(r.o0 / r.prev_close - 1) >= 0.03
       and r.c_0905 is not null and (r.c_0905 - r.prev_close) / (r.o0 - r.prev_close) >= 0.75 then
      ev := ev || (case when r.o0 > r.prev_close then 'gap_up' else 'gap_down' end);
    end if;
    -- 接近漲跌停（距 ±10% 1% 以內，連 2 根）
    if r.c1 >= r.prev_close * 1.09 and r.c2 >= r.prev_close * 1.09 then ev := ev || 'near_limit_up'; end if;
    if r.c1 <= r.prev_close * 0.91 and r.c2 <= r.prev_close * 0.91 then
      ev := ev || 'near_limit_down'; urgent := urgent or r.role = 'holding';
    end if;
    if cardinality(ev) = 0 then continue; end if;

    -- 去重：同事件 30 分鐘冷卻 + 價格再變動門檻；同股 10 分鐘一次；每股每日 3 則（緊急可再 +2）；全日 20 則
    ev := array(select e from unnest(ev) e
                 where not exists (select 1 from intraday_event_state s
                                    where s.for_date = d and s.code = r.code and s.event = e
                                      and (s.last_sent_at > now() - interval '30 minutes'
                                           or abs(r.c1 / s.last_price - 1) < greatest(0.01, 0.5 * coalesce(r.atr, 0) / r.c1))));
    if cardinality(ev) = 0 then continue; end if;
    select coalesce(sum(sent_count), 0) into sent_stock from intraday_event_state where for_date = d and code = r.code;
    if not urgent and (sent_stock >= 3 or day_total >= 20) then continue; end if;
    if urgent and sent_stock >= 5 then continue; end if;
    if not urgent and exists (select 1 from intraday_event_state where for_date = d and code = r.code
                               and last_sent_at > now() - interval '10 minutes') then continue; end if;

    txt := public._event_text(r.code, r.name, r.role, r.etf, r.c1, r.prev_close, r.stop, r.hh20, r.ma20, r.ma60,
                              r.vwap, r.bf, r.atr, least(r.l1, r.l2), r.target, ev);
    if txt is null then continue; end if;
    insert into intraday_outbox (for_date, code, urgent, line_ok, body)
    values (d, r.code, urgent, urgent and r.role = 'holding', txt);
    insert into intraday_event_state (for_date, code, event, last_sent_at, last_price, sent_count)
    select d, r.code, e, now(), r.c1, 1 from unnest(ev) e
    on conflict (for_date, code, event) do update
      set last_sent_at = excluded.last_sent_at, last_price = excluded.last_price,
          sent_count = intraday_event_state.sent_count + 1;
    if not urgent then day_total := day_total + 1; end if;
    n_new := n_new + 1;
  end loop;

  perform public._flush_outbox(extract(minute from tw)::int % 5 = 0);
  return n_new;
end $$;
revoke all on function public.intraday_events() from public, anon, authenticated;

/* 一則事件文字（含建議價） */
create or replace function public._event_text(p_code text, p_name text, p_role text, p_etf boolean, p_c1 numeric,
  p_prev numeric, p_stop numeric, p_hh20 numeric, p_ma20 numeric, p_ma60 numeric, p_vwap numeric, p_bf numeric,
  p_atr numeric, p_low numeric, p_target numeric, ev text[])
returns text
language plpgsql as $$
declare
  role_t text := case p_role when 'holding' then '持股' when 'tracked' then '追蹤中' else '今日推薦' end;
  labels text := '';
  e text;
  m numeric; zone_lo numeric; zone_hi numeric; sb numeric; tgt numeric;
  sell_px numeric;
  plan text := '';
  is_buy boolean := false;
  is_sell boolean := false;
  tw time := (now() at time zone 'Asia/Taipei')::time;
begin
  foreach e in array ev loop
    labels := labels || case e
      when 'stop' then '跌破停損 ' || p_stop
      when 'heavy_sell' then '帶量下殺'
      when 'heavy_rally' then '帶量上漲'
      when 'support_break' then '跌破支撐（月線／季線／20日低）'
      when 'breakout' then '突破 20 日高 ' || p_hh20
      when 'ma20_rebound' then '回測月線止跌'
      when 'gap_up' then '開盤跳空上漲'
      when 'gap_down' then '開盤跳空下跌'
      when 'near_limit_up' then '接近漲停'
      when 'near_limit_down' then '接近跌停'
      else e end || '、';
    if e in ('breakout', 'ma20_rebound', 'heavy_rally', 'gap_up') then is_buy := true; end if;
    if e in ('stop', 'heavy_sell', 'support_break', 'gap_down', 'near_limit_down') then is_sell := true; end if;
  end loop;
  labels := rtrim(labels, '、');

  if is_sell and p_role = 'holding' then
    sell_px := tw_round(p_c1 - greatest(tw_tick(p_c1, p_etf), 0.001 * p_c1), p_etf, 'down');
    plan := '建議：' || (case when 'stop' = any(ev) or 'near_limit_down' = any(ev) then '依停損出場或減碼' else '可先減碼' end)
            || '，參考賣價 ' || sell_px;
  elsif is_sell then
    plan := '建議：暫緩買進（' || role_t || '股轉弱）';
  elsif p_role = 'holding' and is_buy then
    plan := '建議：持股轉強，可續抱；若要獲利了結可分批，參考價 ' || tw_round(p_c1, p_etf, 'near');
  elsif is_buy then
    m := case when 'breakout' = any(ev) then p_hh20 when 'ma20_rebound' = any(ev) then p_ma20 else p_vwap end;
    zone_lo := tw_round(m - p_bf / 2, p_etf, 'down');
    zone_hi := tw_round(m + p_bf / 2, p_etf, 'down');
    sb := tw_round(least(m - greatest(p_bf, 0.5 * coalesce(p_atr, 0)), p_low - p_bf), p_etf, 'down');
    tgt := coalesce(p_target, case when p_ma60 > m then p_ma60 when p_hh20 > m then p_hh20 end, m + 2 * coalesce(p_atr, 0));
    tgt := tw_round(least(tgt, p_prev * 1.095), p_etf, 'down');
    if tw > time '13:20' then
      plan := '建議：已近收盤，今天不追，明天再看';
    elsif p_c1 > p_prev * 1.05 or p_c1 >= p_prev * 1.09 or p_c1 > coalesce(p_vwap, p_c1) + 1.5 * coalesce(p_atr, 0) then
      plan := '建議：漲幅已大，不追高；等拉回 ' || zone_lo || '～' || zone_hi || ' 再考慮';
    elsif (m - sb) > 0.02 * m or (tgt - m) < 2 * (m - sb) then
      plan := '建議：風險報酬不佳（停損距離過大或上檔有限），先觀察';
    else
      plan := '建議買進區間 ' || zone_lo || '～' || zone_hi || '，停損 ' || sb || '，目標 ' || tgt || '（15 分鐘內未成交就放棄）';
    end if;
  end if;
  return format('%s %s（%s）現價 %s（%s%%）｜%s｜%s', p_code, coalesce(p_name, ''), role_t, p_c1,
                to_char(100 * (p_c1 / p_prev - 1), 'FMSG990.0'), labels, plan);
end $$;

/* 送出：緊急事件立即送；一般事件每 5 分鐘一批。成功才標記（失敗下一輪重試） */
create or replace function public._flush_outbox(batch_due boolean)
returns void
language plpgsql security definer set search_path = public, extensions as $$
declare
  d text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD');
  v_ym text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMM');
  hh text := to_char(now() at time zone 'Asia/Taipei', 'HH24:MI');
  ids bigint[]; body text; st int; line_ids bigint[]; line_body text; used int; today_line int;
begin
  select array_agg(id order by urgent desc, id), string_agg(body, E'\n' order by urgent desc, id)
    into ids, body
    from intraday_outbox where for_date = d and not teams_sent and (urgent or batch_due);
  if ids is not null then
    st := _post_json(_vault('teams_webhook_url'), jsonb_build_object('type', 'message', 'attachments', jsonb_build_array(
      jsonb_build_object('contentType', 'application/vnd.microsoft.card.adaptive', 'content', jsonb_build_object(
        '$schema', 'http://adaptivecards.io/schemas/adaptive-card.json', 'type', 'AdaptiveCard', 'version', '1.4',
        'body', jsonb_build_array(jsonb_build_object('type', 'TextBlock', 'text', '⚡ 盤中提醒 ' || hh || E'\n' || body,
                                                     'wrap', true)))))));
    if st between 200 and 299 then
      update intraday_outbox set teams_sent = true where id = any(ids);
    end if;
  end if;
  -- LINE：只發持股緊急事件；每日 ≤5 則、每月 ≤150 則
  select array_agg(id order by id), string_agg(body, E'\n' order by id) into line_ids, line_body
    from intraday_outbox where for_date = d and line_ok and not line_sent;
  if line_ids is not null then
    select count(*) into today_line from intraday_outbox where for_date = d and line_sent;
    insert into alert_quota (ym, line_sent) values (v_ym, 0) on conflict do nothing;
    select line_sent into used from alert_quota where alert_quota.ym = v_ym for update;
    if today_line < 5 and used < 150 and _vault('line_channel_token') is not null then
      st := _post_json('https://api.line.me/v2/bot/message/push',
        jsonb_build_object('to', _vault('line_user_id'), 'messages', jsonb_build_array(jsonb_build_object(
          'type', 'text', 'text', '⚡ 持股盤中 ' || hh || E'\n' || line_body))),
        'Bearer ' || _vault('line_channel_token'));
      if st = 200 then
        update intraday_outbox set line_sent = true where id = any(line_ids);
        update alert_quota set line_sent = line_sent + 1 where alert_quota.ym = v_ym;
      end if;
    end if;
  end if;
end $$;
revoke all on function public._flush_outbox(boolean) from public, anon, authenticated;

/* 盤中抓價也要涵蓋今日 context 的股票（推薦、追蹤、持股） */
create or replace function public.intraday_capture()
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare
  tw timestamp := now() at time zone 'Asia/Taipei';
  codes text[]; chunk text[]; chans text; res record; total int := 0; i int;
begin
  if tw::time < time '08:59' or tw::time > time '13:36' then return 0; end if;
  if not exists (select 1 from intraday_context where for_date = to_char(tw, 'YYYYMMDD'))
     and extract(isodow from tw) > 5 then
    return 0;
  end if;
  select array_agg(code order by pri, code) into codes from (
    select code, min(pri) pri from (
      select code, case role when 'holding' then 0 when 'tracked' then 1 else 2 end pri
        from intraday_context where for_date = to_char(tw, 'YYYYMMDD')
      union all
      select code, 3 from intraday_watch where last_seen > now() - interval '3 days'
    ) u group by code
  ) s where code ~ '^[0-9A-Z]{4,6}$';
  if codes is null then return 0; end if;
  codes := codes[1:200];
  i := 1;
  while i <= array_length(codes, 1) loop
    chunk := codes[i:i + 39];
    select string_agg('tse_' || c || '.tw%7Cotc_' || c || '.tw', '%7C') into chans from unnest(chunk) c;
    select * into res from _market_get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' || chans, 4000);
    if res.status = 200 then
      begin
        total := total + _ingest_mis((res.body::jsonb) -> 'msgArray');
      exception when others then null;
      end;
    end if;
    i := i + 40;
  end loop;
  return total;
end $$;
revoke all on function public.intraday_capture() from public, anon, authenticated;

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname in ('intraday-alerts', 'intraday-events', 'events-cleanup');
  perform cron.schedule('intraday-events', '* * * * *', 'select public.intraday_events()');
  perform cron.schedule('events-cleanup', '55 6 * * *', $c$delete from public.intraday_context where for_date < to_char(now() at time zone 'Asia/Taipei' - interval '14 days', 'YYYYMMDD'); delete from public.intraday_event_state where for_date < to_char(now() at time zone 'Asia/Taipei' - interval '14 days', 'YYYYMMDD'); delete from public.intraday_outbox where created_at < now() - interval '60 days'$c$);
end $$;
