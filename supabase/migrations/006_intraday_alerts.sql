-- 盤中即時提醒：daily-picks（前一晚）寫入隔天要盯的價位 → pg_cron 每分鐘比對自建 1 分 K 最新價
-- → 觸價即通知（Teams 全部；LINE 只發 priority 1，且每月上限保留免費額度）。同一價位一天只通知一次。
-- 金鑰放 Supabase Vault（db-migrate.yml 從 GitHub Secrets 寫入）：teams_webhook_url / line_channel_token / line_user_id

create table if not exists public.alert_levels (
  for_date   text not null,                       -- YYYYMMDD 交易日（台北）
  code       text not null,
  kind       text not null,                       -- stop / take_profit / trailing_stop / ma20_break …
  direction  text not null check (direction in ('above', 'below')),
  price      numeric not null check (price > 0),
  label      text not null,                       -- 例：台積電 追蹤停損
  priority   smallint not null default 2,         -- 1 = 持股出場類（也發 LINE），2 = 其他（Teams）
  created_at timestamptz not null default now(),
  primary key (for_date, code, kind)
);
alter table public.alert_levels enable row level security;

create table if not exists public.alert_log (
  for_date  text not null,
  code      text not null,
  kind      text not null,
  fired_at  timestamptz not null default now(),
  price     numeric,
  result    jsonb,
  primary key (for_date, code, kind)
);
alter table public.alert_log enable row level security;

create table if not exists public.alert_quota (
  ym        text primary key,                     -- YYYYMM
  line_sent int not null default 0
);
alter table public.alert_quota enable row level security;

/* HTTP POST JSON（http extension；逾時毫秒） */
create or replace function public._post_json(url text, body jsonb, auth text default null, timeout_ms int default 4000)
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare r extensions.http_response; hdr extensions.http_header[] := array[]::extensions.http_header[];
begin
  if url is null or url = '' then return null; end if;
  begin
    perform extensions.http_set_curlopt('CURLOPT_TIMEOUT_MS', timeout_ms::text);
  exception when others then null;
  end;
  if auth is not null then hdr := array[extensions.http_header('Authorization', auth)]; end if;
  r := extensions.http(('POST', url, hdr, 'application/json', body::text)::extensions.http_request);
  return r.status;
exception when others then
  return -1;
end $$;
revoke all on function public._post_json(text, jsonb, text, int) from public, anon, authenticated;

create or replace function public._vault(name text)
returns text
language sql security definer set search_path = public as $$
  select decrypted_secret from vault.decrypted_secrets where vault.decrypted_secrets.name = _vault.name limit 1
$$;
revoke all on function public._vault(text) from public, anon, authenticated;

/* 每分鐘：比對今日價位，觸價者記錄並通知（批次一則訊息） */
create or replace function public.intraday_alerts()
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare
  tw timestamp := now() at time zone 'Asia/Taipei';
  d text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD');
  v_ym text := to_char(now() at time zone 'Asia/Taipei', 'YYYYMM');
  hits jsonb;
  n int;
  txt text;
  line_txt text;
  teams_url text;
  line_token text;
  line_user text;
  st_teams int;
  st_line int;
  line_cap int := 150;          -- 每月 LINE 盤中提醒上限（免費約 200 則，留給每日報告）
  used int;
begin
  if extract(isodow from tw) > 6 or tw::time < time '09:00' or tw::time > time '13:31' then
    return 0;
  end if;
  with latest as (                                -- 每檔最新一分鐘（3 分鐘內才算有效報價）
    select distinct on (code) code, close, minute
      from intraday_bars
     where minute >= now() - interval '3 minutes'
     order by code, minute desc
  ), hit as (
    select a.*, l.close as now_price
      from alert_levels a
      join latest l on l.code = a.code
     where a.for_date = d
       and ((a.direction = 'below' and l.close <= a.price) or (a.direction = 'above' and l.close >= a.price))
       and not exists (select 1 from alert_log g where g.for_date = a.for_date and g.code = a.code and g.kind = a.kind)
  ), ins as (
    insert into alert_log (for_date, code, kind, price)
    select for_date, code, kind, now_price from hit
    on conflict do nothing
    returning code, kind
  )
  select jsonb_agg(jsonb_build_object('code', h.code, 'label', h.label, 'price', h.price, 'now', h.now_price,
                                      'dir', h.direction, 'p', h.priority) order by h.priority, h.code)
    into hits
    from hit h join ins i on i.code = h.code and i.kind = h.kind;
  n := coalesce(jsonb_array_length(hits), 0);
  if n = 0 then return 0; end if;

  select string_agg(format('%s %s %s %s（現價 %s）', x->>'code', x->>'label',
                           case when x->>'dir' = 'below' then '跌破' else '突破' end, x->>'price', x->>'now'), E'\n')
    into txt from jsonb_array_elements(hits) x;
  txt := format('⚡ 盤中提醒 %s', to_char(tw, 'HH24:MI')) || E'\n' || txt;

  teams_url := _vault('teams_webhook_url');
  st_teams := _post_json(teams_url, jsonb_build_object('type', 'message', 'attachments', jsonb_build_array(
    jsonb_build_object('contentType', 'application/vnd.microsoft.card.adaptive', 'content', jsonb_build_object(
      '$schema', 'http://adaptivecards.io/schemas/adaptive-card.json', 'type', 'AdaptiveCard', 'version', '1.4',
      'body', jsonb_build_array(jsonb_build_object('type', 'TextBlock', 'text', txt, 'wrap', true)))))));

  select string_agg(format('%s %s %s %s（%s）', x->>'code', x->>'label',
                           case when x->>'dir' = 'below' then '跌破' else '突破' end, x->>'price', x->>'now'), E'\n')
    into line_txt from jsonb_array_elements(hits) x where (x->>'p')::int = 1;
  if line_txt is not null then
    insert into alert_quota (ym, line_sent) values (v_ym, 0) on conflict do nothing;
    select line_sent into used from alert_quota where alert_quota.ym = v_ym for update;
    if used < line_cap then
      line_token := _vault('line_channel_token');
      line_user := _vault('line_user_id');
      if line_token is not null and line_user is not null then
        st_line := _post_json('https://api.line.me/v2/bot/message/push',
          jsonb_build_object('to', line_user, 'messages', jsonb_build_array(jsonb_build_object(
            'type', 'text', 'text', format('⚡ 盤中 %s', to_char(tw, 'HH24:MI')) || E'\n' || line_txt))),
          'Bearer ' || line_token);
        if st_line = 200 then
          update alert_quota set line_sent = line_sent + 1 where alert_quota.ym = v_ym;
        end if;
      end if;
    end if;
  end if;
  update alert_log g set result = jsonb_build_object('teams', st_teams, 'line', st_line)
   where g.for_date = d and g.result is null;   -- the rows inserted in this run (earlier ones already have a result)
  return n;
end $$;
revoke all on function public.intraday_alerts() from public, anon, authenticated;

/* 盤中抓價也要涵蓋今日有提醒價位的股票（推薦股可能沒人開過網頁） */
create or replace function public.intraday_capture()
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare
  tw timestamp := now() at time zone 'Asia/Taipei';
  codes text[];
  chunk text[];
  chans text;
  res record;
  total int := 0;
  i int;
begin
  if extract(isodow from tw) > 6 or tw::time < time '08:59' or tw::time > time '13:36' then
    return 0;
  end if;
  select array_agg(code order by pri, code) into codes from (
    select code, min(pri) pri from (
      select code, 0 pri from alert_levels where for_date = to_char(tw, 'YYYYMMDD')
      union all
      select distinct upper(stock_id), 1 from transactions
      union all
      select code, 2 from intraday_watch where last_seen > now() - interval '3 days'
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
  perform cron.unschedule(jobid) from cron.job where jobname in ('intraday-alerts', 'alerts-cleanup');
  perform cron.schedule('intraday-alerts', '* * * * *', 'select public.intraday_alerts()');
  perform cron.schedule('alerts-cleanup', '45 6 * * *', $c$delete from public.alert_levels where for_date < to_char(now() at time zone 'Asia/Taipei' - interval '14 days', 'YYYYMMDD'); delete from public.alert_log where for_date < to_char(now() at time zone 'Asia/Taipei' - interval '60 days', 'YYYYMMDD')$c$);
end $$;
