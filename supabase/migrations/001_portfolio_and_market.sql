-- 持股帳本 + 盤中報價轉發（由 .github/workflows/db-migrate.yml 套用，可重複執行）
-- 前端用 anon key 經 PostgREST 呼叫；所有表格都有 RLS，只能讀寫自己的資料。

create extension if not exists http with schema extensions;

/* ── 交易紀錄 ─────────────────────────────────────────────────────── */
create table if not exists public.transactions (
  id          uuid primary key default gen_random_uuid(),
  user_id     uuid not null default auth.uid() references auth.users(id) on delete cascade,
  trade_date  date not null,
  stock_id    text not null,
  stock_name  text,
  side        text not null check (side in ('buy','sell','cash_dividend','stock_dividend')),
  shares      numeric not null default 0,      -- 股數（不是張）
  price       numeric not null default 0,      -- 成交價；現金股利時為每股配息
  fee         numeric not null default 0,      -- 手續費
  tax         numeric not null default 0,      -- 證交稅
  amount      numeric,                          -- 券商列示淨收付金額（可空，僅供對帳）
  broker      text,
  source      text not null default 'manual',  -- manual / csv / watchlist
  import_hash text,                             -- CSV 去重用
  note        text,
  created_at  timestamptz not null default now()
);
create index if not exists transactions_user_date on public.transactions (user_id, trade_date);
create unique index if not exists transactions_user_hash on public.transactions (user_id, import_hash) where import_hash is not null;

alter table public.transactions enable row level security;
drop policy if exists "own transactions" on public.transactions;
create policy "own transactions" on public.transactions
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

/* ── 個股設定（存股 / 波段） ─────────────────────────────────────── */
create table if not exists public.holding_settings (
  user_id    uuid not null default auth.uid() references auth.users(id) on delete cascade,
  stock_id   text not null,
  style      text not null default '存股' check (style in ('存股','波段')),
  note       text,
  updated_at timestamptz not null default now(),
  primary key (user_id, stock_id)
);
alter table public.holding_settings enable row level security;
drop policy if exists "own settings" on public.holding_settings;
create policy "own settings" on public.holding_settings
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

/* ── CSV 匯入欄位對應預設 ───────────────────────────────────────── */
create table if not exists public.import_presets (
  user_id    uuid not null default auth.uid() references auth.users(id) on delete cascade,
  name       text not null,
  mapping    jsonb not null,
  updated_at timestamptz not null default now(),
  primary key (user_id, name)
);
alter table public.import_presets enable row level security;
drop policy if exists "own presets" on public.import_presets;
create policy "own presets" on public.import_presets
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

/* ── 報價快取（只給下面的 security definer 函式用，無 policy = 前端讀不到） ── */
create table if not exists public.market_cache (
  key        text primary key,
  payload    jsonb not null,
  fetched_at timestamptz not null default now()
);
alter table public.market_cache enable row level security;

/* ── 內部：GET 一個網址，回 (status, body)；失敗回 status 0 ─────────── */
create or replace function public._market_get(url text, timeout_ms int default 2500)
returns table (status int, body text)
language plpgsql security definer set search_path = public, extensions as $$
declare r extensions.http_response;
begin
  begin
    perform extensions.http_set_curlopt('CURLOPT_TIMEOUT_MS', timeout_ms::text);
  exception when others then null;
  end;
  select * into r from extensions.http((
    'GET', url,
    array[extensions.http_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'),
          extensions.http_header('Referer', 'https://mis.twse.com.tw/stock/index.jsp')],
    null, null)::extensions.http_request);
  return query select r.status, r.content;
exception when others then
  return query select 0, sqlerrm;
end $$;
revoke all on function public._market_get(text, int) from public, anon, authenticated;

/* ── 盤中即時報價：TWSE MIS（上市＋上櫃一次查），失敗退 Yahoo ───────
   回傳 { source, fetched_at, quotes: [{code,name,price,prev,open,high,low,vol,time,bids,asks}] } */
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

  select payload into hit from market_cache where key = ckey and fetched_at > now() - interval '4 seconds';
  if hit is not null then return hit; end if;

  select string_agg('tse_' || x || '.tw%7Cotc_' || x || '.tw', '%7C') into chans from unnest(clean) x;
  select * into res from _market_get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' || chans);

  if res.status = 200 then
    begin
      arr := (res.body::jsonb) -> 'msgArray';
    exception when others then arr := null;
    end;
  end if;

  if arr is not null and jsonb_array_length(arr) > 0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',  m->>'c',
      'name',  m->>'n',
      'market', m->>'ex',
      'price', nullif(m->>'z', '-'),
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
    from jsonb_array_elements(arr) m;
  else
    -- 備援：Yahoo 每檔一次（.TW 不到再試 .TWO）
    src := 'Yahoo';
    foreach c in array clean[1:10] loop
      y := null;
      select * into res from _market_get('https://query1.finance.yahoo.com/v8/finance/chart/' || c || '.TW?interval=1d&range=1d');
      if res.status <> 200 then
        select * into res from _market_get('https://query1.finance.yahoo.com/v8/finance/chart/' || c || '.TWO?interval=1d&range=1d');
      end if;
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
  return res_json;
end $$;

/* ── 分時 / 多日分K：Yahoo chart（interval 1m/5m/15m/60m） ────────────
   回傳 { symbol, interval, prev_close, t:[unix秒], o:[], h:[], l:[], c:[], v:[] } */
create or replace function public.market_intraday(code text, intv text default '1m', rng text default '1d')
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare
  ckey text; hit jsonb; res record; r jsonb; q jsonb; sym text; res_json jsonb; suffix text;
begin
  code := upper(trim(code));
  if code !~ '^[0-9A-Z]{4,6}$' then return null; end if;
  if intv not in ('1m','2m','5m','15m','30m','60m') then intv := '1m'; end if;
  if rng not in ('1d','5d','1mo') then rng := '1d'; end if;
  ckey := 'i:' || code || ':' || intv || ':' || rng;

  select payload into hit from market_cache where key = ckey and fetched_at > now() - interval '20 seconds';
  if hit is not null then return hit; end if;

  foreach suffix in array array['.TW', '.TWO'] loop
    sym := code || suffix;
    select * into res from _market_get('https://query1.finance.yahoo.com/v8/finance/chart/' || sym || '?interval=' || intv || '&range=' || rng, 4000);
    if res.status = 200 then
      begin
        r := (res.body::jsonb) #> '{chart,result,0}';
      exception when others then r := null;
      end;
      exit when r is not null and jsonb_array_length(coalesce(r->'timestamp', '[]'::jsonb)) > 0;
    end if;
    r := null;
  end loop;
  if r is null then return null; end if;

  q := r #> '{indicators,quote,0}';
  res_json := jsonb_build_object(
    'symbol', sym, 'interval', intv, 'range', rng,
    'prev_close', r #> '{meta,chartPreviousClose}',
    'price', r #> '{meta,regularMarketPrice}',
    't', r->'timestamp', 'o', q->'open', 'h', q->'high', 'l', q->'low', 'c', q->'close', 'v', q->'volume');
  insert into market_cache(key, payload, fetched_at) values (ckey, res_json, now())
    on conflict (key) do update set payload = excluded.payload, fetched_at = excluded.fetched_at;
  return res_json;
end $$;

/* ── AI 解說：Groq（key 放在 Supabase Vault，名稱 groq_api_key）；限登入者，每人每小時 30 次 ── */
create table if not exists public.ai_usage (
  user_id uuid not null,
  used_at timestamptz not null default now()
);
create index if not exists ai_usage_user_time on public.ai_usage (user_id, used_at);
alter table public.ai_usage enable row level security;

create or replace function public.ai_explain(prompt text)
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare
  k text; r extensions.http_response; body jsonb; uid uuid := auth.uid(); n int;
begin
  if uid is null then return jsonb_build_object('error', '請先登入'); end if;
  select count(*) into n from ai_usage where user_id = uid and used_at > now() - interval '1 hour';
  if n >= 30 then return jsonb_build_object('error', '本小時 AI 次數已達上限（30 次）'); end if;
  begin
    select decrypted_secret into k from vault.decrypted_secrets where name = 'groq_api_key' limit 1;
  exception when others then k := null;
  end;
  if k is null then return jsonb_build_object('error', 'AI 尚未啟用（Vault 缺 groq_api_key）'); end if;
  insert into ai_usage(user_id) values (uid);
  delete from ai_usage where used_at < now() - interval '1 day';

  begin
    perform extensions.http_set_curlopt('CURLOPT_TIMEOUT_MS', '7000');
  exception when others then null;
  end;
  select * into r from extensions.http((
    'POST', 'https://api.groq.com/openai/v1/chat/completions',
    array[extensions.http_header('Authorization', 'Bearer ' || k)],
    'application/json',
    jsonb_build_object(
      'model', 'llama-3.3-70b-versatile',
      'temperature', 0.3,
      'max_tokens', 700,
      'messages', jsonb_build_array(
        jsonb_build_object('role', 'system', 'content',
          '你是台股投資助理。用繁體中文、台灣用語，根據使用者提供的規則引擎結果，用白話解說進出場建議與理由。不得自行編造數字，只能引用提供的數據。最後提醒僅供參考。'),
        jsonb_build_object('role', 'user', 'content', left(prompt, 6000))))::text
  )::extensions.http_request);
  if r.status <> 200 then return jsonb_build_object('error', 'AI 服務回應 ' || r.status); end if;
  body := r.content::jsonb;
  return jsonb_build_object('text', body #>> '{choices,0,message,content}', 'model', body->>'model');
exception when others then
  return jsonb_build_object('error', sqlerrm);
end $$;

revoke all on function public.market_quote(text[]) from public;
revoke all on function public.market_intraday(text, text, text) from public;
revoke all on function public.ai_explain(text) from public;
grant execute on function public.market_quote(text[]) to anon, authenticated;
grant execute on function public.market_intraday(text, text, text) to anon, authenticated;
grant execute on function public.ai_explain(text) to authenticated;

notify pgrst, 'reload schema';
