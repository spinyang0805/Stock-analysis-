-- 自建盤中分時：Yahoo 台股分時盤中延遲、MIS 個股無分時 API
-- → pg_cron 盤中每 20 秒抓 MIS 快照，彙整成 1 分 K（intraday_bars）
-- 追蹤對象：近 3 天有人看過的股票（market_quote 自動登記）＋ 交易紀錄裡的股票

create extension if not exists pg_cron;

create table if not exists public.intraday_watch (
  code       text primary key,
  last_seen  timestamptz not null default now()
);
alter table public.intraday_watch enable row level security;

create table if not exists public.intraday_bars (
  code      text not null,
  minute    timestamptz not null,
  open      numeric not null,
  high      numeric not null,
  low       numeric not null,
  close     numeric not null,
  cum_vol   bigint not null default 0,   -- 當日累計成交量（張）
  primary key (code, minute)
);
alter table public.intraday_bars enable row level security;

/* MIS msgArray → 寫入 1 分 K（只收台北「今天」的成交） */
create or replace function public._ingest_mis(arr jsonb)
returns int
language plpgsql security definer set search_path = public, extensions as $$
declare n int := 0;
begin
  if arr is null then return 0; end if;
  with src as (
    select m->>'c' as code,
           coalesce(nullif(m->>'z', '-'), nullif(m->>'pz', '-'))::numeric as price,
           nullif(m->>'v', '-')::bigint as vol,
           to_timestamp((m->>'tlong')::bigint / 1000.0) as ts,
           m->>'d' as d
      from jsonb_array_elements(arr) m
     where coalesce(m->>'c', '') <> '' and m->>'tlong' ~ '^[0-9]+$'
  ), ok as (
    select code, price, coalesce(vol, 0) as vol, date_trunc('minute', ts) as minute
      from src
     where price > 0
       and d = to_char(now() at time zone 'Asia/Taipei', 'YYYYMMDD')
  ), ins as (
    insert into intraday_bars as b (code, minute, open, high, low, close, cum_vol)
    select code, minute, price, price, price, price, vol from ok
    on conflict (code, minute) do update set
      high = greatest(b.high, excluded.high),
      low = least(b.low, excluded.low),
      close = excluded.close,
      cum_vol = greatest(b.cum_vol, excluded.cum_vol)
    returning 1
  )
  select count(*) into n from ins;
  return n;
end $$;
revoke all on function public._ingest_mis(jsonb) from public, anon, authenticated;

/* 排程：盤中抓追蹤清單的 MIS 快照 */
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
  if extract(isodow from tw) > 5 or tw::time < time '08:59' or tw::time > time '13:36' then
    return 0;
  end if;
  select array_agg(code order by code) into codes from (
    select code from intraday_watch where last_seen > now() - interval '3 days'
    union
    select distinct upper(stock_id) from transactions
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

/* 今日 1 分 K（給 market_intraday 用）：t 為 unix 秒，v 為每分鐘量（股） */
create or replace function public._intraday_today(p_code text, bucket_min int default 1)
returns jsonb
language sql security definer set search_path = public as $$
  with b as (
    select code, minute, open, high, low, close, cum_vol,
           cum_vol - coalesce(lag(cum_vol) over (order by minute), 0) as vol
      from intraday_bars
     where code = p_code
       and minute >= ((now() at time zone 'Asia/Taipei')::date::timestamp at time zone 'Asia/Taipei')
  ), g as (
    select to_timestamp(floor(extract(epoch from minute) / (bucket_min * 60)) * bucket_min * 60) as bucket,
           (array_agg(open order by minute))[1] as o, max(high) as h, min(low) as l,
           (array_agg(close order by minute desc))[1] as c, sum(greatest(vol, 0)) * 1000 as v
      from b group by 1
  )
  select case when count(*) = 0 then null else jsonb_build_object(
    't', jsonb_agg(extract(epoch from bucket)::bigint order by bucket),
    'o', jsonb_agg(o order by bucket), 'h', jsonb_agg(h order by bucket),
    'l', jsonb_agg(l order by bucket), 'c', jsonb_agg(c order by bucket),
    'v', jsonb_agg(v order by bucket)) end
  from g;
$$;
revoke all on function public._intraday_today(text, int) from public, anon, authenticated;

/* market_quote：登記追蹤、順手寫入分時 */
create or replace function public.market_quote(codes text[])
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare
  clean   text[];
  ckey    text;
  hit     jsonb;
  chans   text;
  res     record;
  arr     jsonb;
  quotes  jsonb := '[]'::jsonb;
  src     text := 'TWSE MIS';
  c       text;
  y       jsonb;
  res_json jsonb;
begin
  select array_agg(x order by x) into clean
    from (select distinct upper(trim(v)) x from unnest(codes) v) s
   where x ~ '^[0-9A-Z]{4,6}$';
  if clean is null then return jsonb_build_object('source', null, 'quotes', '[]'::jsonb); end if;
  clean := clean[1:40];
  ckey := 'q:' || array_to_string(clean, ',');

  -- 登記到分時追蹤清單（每分鐘最多更新一次）
  insert into intraday_watch(code, last_seen) select x, now() from unnest(clean) x
    on conflict (code) do update set last_seen = excluded.last_seen
    where intraday_watch.last_seen < now() - interval '1 minute';

  select payload into hit from market_cache where key = ckey and fetched_at > now() - interval '4 seconds';
  if hit is not null then return hit; end if;

  select string_agg('tse_' || x || '.tw%7Cotc_' || x || '.tw', '%7C') into chans from unnest(clean) x;
  -- anon 角色 statement_timeout 只有 3 秒：MIS 給 1.8 秒，備援 Yahoo 最多 2 檔各 0.5 秒
  select * into res from _market_get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' || chans, 1800);

  if res.status = 200 then
    begin
      arr := (res.body::jsonb) -> 'msgArray';
    exception when others then arr := null;
    end;
  end if;

  if arr is not null and jsonb_array_length(arr) > 0 then
    begin
      perform _ingest_mis(arr);
    exception when others then null;
    end;
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',  m->>'c',
      'name',  m->>'n',
      'market', m->>'ex',
      'price', coalesce(nullif(m->>'z', '-'), nullif(m->>'pz', '-')),
      'prev',  m->>'y',
      'open',  nullif(m->>'o', '-'),
      'high',  nullif(m->>'h', '-'),
      'low',   nullif(m->>'l', '-'),
      'vol',   m->>'v',
      'date',  m->>'d',
      'time',  m->>'t',
      'bids',  m->>'b', 'bidVols', m->>'g',
      'asks',  m->>'a', 'askVols', m->>'f'
    )), '[]'::jsonb) into quotes
    from jsonb_array_elements(arr) m
    where coalesce(m->>'c', '') <> '';
  else
    src := 'Yahoo';
    foreach c in array clean[1:2] loop
      y := null;
      select * into res from _market_get('https://query1.finance.yahoo.com/v8/finance/chart/' || c || '.TW?interval=1d&range=1d', 500);
      if res.status = 200 then
        begin
          y := (res.body::jsonb) #> '{chart,result,0,meta}';
        exception when others then y := null;
        end;
      end if;
      if y is not null then
        quotes := quotes || jsonb_build_array(jsonb_build_object(
          'code', c, 'name', y->>'shortName',
          'price', y->>'regularMarketPrice', 'prev', y->>'chartPreviousClose',
          'high', y->>'regularMarketDayHigh', 'low', y->>'regularMarketDayLow',
          'vol', ((y->>'regularMarketVolume')::numeric / 1000)::bigint::text,
          'time', to_char(to_timestamp((y->>'regularMarketTime')::bigint) at time zone 'Asia/Taipei', 'HH24:MI:SS')));
      end if;
    end loop;
  end if;

  res_json := jsonb_build_object('source', src, 'fetched_at', now(), 'quotes', quotes);
  if jsonb_array_length(quotes) > 0 then
    insert into market_cache(key, payload, fetched_at) values (ckey, res_json, now())
      on conflict (key) do update set payload = excluded.payload, fetched_at = excluded.fetched_at;
  end if;
  if random() < 0.02 then
    delete from market_cache where fetched_at < now() - interval '1 hour';
  end if;
  return res_json;
end $$;

/* market_intraday：盤中優先用自建 1 分 K；5 分 K 把今天自建資料接在 Yahoo 歷史後面 */
create or replace function public.market_intraday(code text, intv text default '1m', rng text default '1d', mkt text default null)
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare
  ckey text; hit jsonb; res record; r jsonb; q jsonb; sym text; res_json jsonb; suffix text; suffixes text[];
  own jsonb; tw timestamp := now() at time zone 'Asia/Taipei'; in_session boolean; bucket int; last_t bigint; idx int;
begin
  code := upper(trim(code));
  if code !~ '^[0-9A-Z]{4,6}$' then return null; end if;
  if intv not in ('1m','2m','5m','15m','30m','60m') then intv := '1m'; end if;
  if rng not in ('1d','5d','1mo') then rng := '1d'; end if;
  ckey := 'i:' || code || ':' || intv || ':' || rng;

  insert into intraday_watch(code, last_seen) values (code, now())
    on conflict (code) do update set last_seen = excluded.last_seen
    where intraday_watch.last_seen < now() - interval '1 minute';

  select payload into hit from market_cache where key = ckey and fetched_at > now() - interval '20 seconds';
  if hit is not null then return hit; end if;

  in_session := extract(isodow from tw) <= 5 and tw::time between time '08:59' and time '13:40';
  bucket := case intv when '1m' then 1 when '2m' then 2 when '5m' then 5 when '15m' then 15 when '30m' then 30 else 60 end;
  own := _intraday_today(code, bucket);

  -- 盤中的當日分時：直接用自建資料（Yahoo 盤中延遲）
  if rng = '1d' and own is not null and in_session then
    res_json := own || jsonb_build_object('symbol', code, 'interval', intv, 'range', rng, 'source', 'MIS',
      'prev_close', (select (m->>'prev')::numeric from market_cache mc, jsonb_array_elements(mc.payload->'quotes') m
                      where mc.key like 'q:%' and m->>'code' = code order by mc.fetched_at desc limit 1));
    insert into market_cache(key, payload, fetched_at) values (ckey, res_json, now())
      on conflict (key) do update set payload = excluded.payload, fetched_at = excluded.fetched_at;
    return res_json;
  end if;

  suffixes := case when mkt in ('上櫃', 'otc', 'TPEx') then array['.TWO', '.TW'] else array['.TW', '.TWO'] end;
  foreach suffix in array suffixes loop
    sym := code || suffix;
    select * into res from _market_get('https://query1.finance.yahoo.com/v8/finance/chart/' || sym || '?interval=' || intv || '&range=' || rng, 1300);
    if res.status = 200 then
      begin
        r := (res.body::jsonb) #> '{chart,result,0}';
      exception when others then r := null;
      end;
      exit when r is not null and jsonb_array_length(coalesce(r->'timestamp', '[]'::jsonb)) > 0;
    end if;
    r := null;
  end loop;

  if r is null then
    if own is null then return null; end if;
    res_json := own || jsonb_build_object('symbol', code, 'interval', intv, 'range', rng, 'source', 'MIS');
  else
    q := r #> '{indicators,quote,0}';
    res_json := jsonb_build_object(
      'symbol', sym, 'interval', intv, 'range', rng, 'source', 'Yahoo',
      'prev_close', r #> '{meta,chartPreviousClose}',
      'price', r #> '{meta,regularMarketPrice}',
      't', r->'timestamp', 'o', q->'open', 'h', q->'high', 'l', q->'low', 'c', q->'close', 'v', q->'volume');
    -- Yahoo 尚未有的今日 K 棒，用自建資料補在後面
    if own is not null then
      last_t := (res_json->'t'->>(jsonb_array_length(res_json->'t') - 1))::bigint;
      for idx in 0 .. jsonb_array_length(own->'t') - 1 loop
        if (own->'t'->>idx)::bigint > last_t then
          res_json := jsonb_set(res_json, '{t}', (res_json->'t') || (own->'t'->idx));
          res_json := jsonb_set(res_json, '{o}', (res_json->'o') || (own->'o'->idx));
          res_json := jsonb_set(res_json, '{h}', (res_json->'h') || (own->'h'->idx));
          res_json := jsonb_set(res_json, '{l}', (res_json->'l') || (own->'l'->idx));
          res_json := jsonb_set(res_json, '{c}', (res_json->'c') || (own->'c'->idx));
          res_json := jsonb_set(res_json, '{v}', (res_json->'v') || (own->'v'->idx));
        end if;
      end loop;
    end if;
  end if;
  insert into market_cache(key, payload, fetched_at) values (ckey, res_json, now())
    on conflict (key) do update set payload = excluded.payload, fetched_at = excluded.fetched_at;
  return res_json;
end $$;

revoke all on function public.market_quote(text[]) from public;
revoke all on function public.market_intraday(text, text, text, text) from public;
grant execute on function public.market_quote(text[]) to anon, authenticated;
grant execute on function public.market_intraday(text, text, text, text) to anon, authenticated;

/* 排程（重跑 migration 時先移除再建立） */
do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname in ('intraday-capture', 'intraday-cleanup');
  perform cron.schedule('intraday-capture', '20 seconds', 'select public.intraday_capture()');
  perform cron.schedule('intraday-cleanup', '30 6 * * *', $c$delete from public.intraday_bars where minute < now() - interval '7 days'; delete from public.intraday_watch where last_seen < now() - interval '14 days'$c$);
end $$;

notify pgrst, 'reload schema';
