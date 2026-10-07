-- AI 選股搬離 Fly：盤後 pg_cron 預先算好候選清單（ai_screen），ai_pick 只呼叫 Groq 一次
-- Groq 已於 2026-08-16 下架 llama-3.3-70b-versatile → 改用 qwen/qwen3.8-27b（Groq Preview 模型）

create table if not exists public.ai_screen (
  signal     text primary key,
  label      text not null,
  rows       jsonb not null,
  data_date  text,
  updated_at timestamptz not null default now()
);
alter table public.ai_screen enable row level security;

/* 盤後選股掃描：近 30 個日曆日的日K + 籌碼 */
create or replace function public.ai_screen_refresh()
returns int
language plpgsql security definer set search_path = public as $$
declare
  cutoff text := to_char((now() at time zone 'Asia/Taipei')::date - 45, 'YYYYMMDD');
  n int := 0;
begin
  create temp table _k on commit drop as
  select stock_id, date, close, volume, market,
         row_number() over (partition by stock_id order by date desc) as rn
    from stock_daily
   where date >= cutoff and close > 0 and stock_id ~ '^[0-9]{4}$';   -- 只看一般股票（排除 ETF／權證）

  create temp table _f on commit drop as
  select k.stock_id,
         max(k.market) as market,
         max(case when rn = 1 then close end) as c1,
         max(case when rn = 2 then close end) as c2,
         max(case when rn = 6 then close end) as c6,
         max(case when rn = 1 then volume end) as v1,
         avg(case when rn between 2 and 6 then volume end) as v5,
         avg(case when rn between 1 and 20 then close end) as ma20,
         avg(case when rn between 2 and 21 then close end) as ma20p,
         max(case when rn = 1 then date end) as d1,
         count(*) as n
    from _k k where rn <= 21
   group by k.stock_id having count(*) >= 15;

  create temp table _c on commit drop as
  select stock_id,
         sum(coalesce(foreign_buy, 0)) filter (where rn <= 5) as f5,
         sum(coalesce(investment_trust_buy, 0)) filter (where rn <= 5) as t5,
         count(*) filter (where rn <= 5 and coalesce(foreign_buy, 0) + coalesce(investment_trust_buy, 0) > 0) as buy_days
    from (select stock_id, foreign_buy, investment_trust_buy,
                 row_number() over (partition by stock_id order by date desc) rn
            from chip_daily where date >= cutoff) x
   where rn <= 5 group by stock_id;

  delete from ai_screen;

  insert into ai_screen(signal, label, rows, data_date)
  select 'vol_surge_up', '量增價漲（今日漲、量 > 5 日均量 1.5 倍）',
         coalesce(jsonb_agg(r order by (r->>'vol_ratio')::numeric desc), '[]'), max(d1)
    from (select jsonb_build_object('code', f.stock_id, 'name', p.name, 'close', f.c1,
                   'chg_pct', round(((f.c1 / f.c2 - 1) * 100)::numeric, 2), 'vol_ratio', round((f.v1 / nullif(f.v5, 0))::numeric, 2)) r, f.d1
            from _f f left join product_universe p on p.code = f.stock_id
           where f.c1 > f.c2 and f.v1 > 1.5 * f.v5 and f.v1 >= 1000000
           order by f.v1 / nullif(f.v5, 0) desc limit 20) s;

  insert into ai_screen(signal, label, rows, data_date)
  select 'top_gainers_5d', '近 5 日漲幅最大',
         coalesce(jsonb_agg(r order by (r->>'chg5_pct')::numeric desc), '[]'), max(d1)
    from (select jsonb_build_object('code', f.stock_id, 'name', p.name, 'close', f.c1,
                   'chg5_pct', round(((f.c1 / f.c6 - 1) * 100)::numeric, 2)) r, f.d1
            from _f f left join product_universe p on p.code = f.stock_id
           where f.c6 > 0 and f.v5 >= 500000
           order by f.c1 / f.c6 desc limit 20) s;

  insert into ai_screen(signal, label, rows, data_date)
  select 'oversold_5d', '近 5 日跌幅最大（可能超賣反彈）',
         coalesce(jsonb_agg(r order by (r->>'chg5_pct')::numeric), '[]'), max(d1)
    from (select jsonb_build_object('code', f.stock_id, 'name', p.name, 'close', f.c1,
                   'chg5_pct', round(((f.c1 / f.c6 - 1) * 100)::numeric, 2)) r, f.d1
            from _f f left join product_universe p on p.code = f.stock_id
           where f.c6 > 0 and f.v5 >= 500000
           order by f.c1 / f.c6 asc limit 20) s;

  insert into ai_screen(signal, label, rows, data_date)
  select 'break_ma20', '今日站上月線（前一日在月線下）',
         coalesce(jsonb_agg(r), '[]'), max(d1)
    from (select jsonb_build_object('code', f.stock_id, 'name', p.name, 'close', f.c1,
                   'ma20', round(f.ma20::numeric, 2), 'chg_pct', round(((f.c1 / f.c2 - 1) * 100)::numeric, 2)) r, f.d1
            from _f f left join product_universe p on p.code = f.stock_id
           where f.c1 > f.ma20 and f.c2 <= f.ma20p and f.v5 >= 500000
           order by f.v1 desc limit 20) s;

  insert into ai_screen(signal, label, rows, data_date)
  select 'institution_buy', '外資＋投信近 5 日買超最多（張）',
         coalesce(jsonb_agg(r order by (r->>'inst5_lots')::numeric desc), '[]'), max(d1)
    from (select jsonb_build_object('code', c.stock_id, 'name', p.name, 'close', f.c1,
                   'inst5_lots', round((c.f5 + c.t5) / 1000.0), 'foreign5_lots', round(c.f5 / 1000.0),
                   'trust5_lots', round(c.t5 / 1000.0), 'buy_days', c.buy_days,
                   'chg5_pct', round(((f.c1 / nullif(f.c6, 0) - 1) * 100)::numeric, 2)) r, f.d1
            from _c c join _f f on f.stock_id = c.stock_id left join product_universe p on p.code = c.stock_id
           where c.f5 + c.t5 > 0
           order by c.f5 + c.t5 desc limit 20) s;

  insert into ai_screen(signal, label, rows, data_date)
  select 'quiet_accumulate', '法人連買但股價還沒漲（5 日漲幅 < 3%）',
         coalesce(jsonb_agg(r order by (r->>'buy_days')::int desc, (r->>'inst5_lots')::numeric desc), '[]'), max(d1)
    from (select jsonb_build_object('code', c.stock_id, 'name', p.name, 'close', f.c1, 'buy_days', c.buy_days,
                   'inst5_lots', round((c.f5 + c.t5) / 1000.0),
                   'chg5_pct', round(((f.c1 / nullif(f.c6, 0) - 1) * 100)::numeric, 2)) r, f.d1
            from _c c join _f f on f.stock_id = c.stock_id left join product_universe p on p.code = c.stock_id
           where c.buy_days >= 4 and f.c6 > 0 and f.c1 / f.c6 < 1.03 and c.f5 + c.t5 > 0
           order by c.buy_days desc, c.f5 + c.t5 desc limit 20) s;

  select count(*) into n from ai_screen;
  return n;
end $$;
revoke all on function public.ai_screen_refresh() from public, anon, authenticated;

/* 共用：呼叫 Groq（key 在 Vault） */
create or replace function public._groq_chat(messages jsonb, max_tokens int default 900, timeout_ms int default 7000)
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare k text; r extensions.http_response; body jsonb;
begin
  begin
    select decrypted_secret into k from vault.decrypted_secrets where name = 'groq_api_key' limit 1;
  exception when others then k := null;
  end;
  if k is null then return jsonb_build_object('error', 'AI 尚未啟用（Vault 缺 groq_api_key）'); end if;
  begin
    begin
      perform extensions.http_set_curlopt('CURLOPT_TIMEOUT_MS', timeout_ms::text);
    exception when others then null;
    end;
    select * into r from extensions.http((
      'POST', 'https://api.groq.com/openai/v1/chat/completions',
      array[extensions.http_header('Authorization', 'Bearer ' || k)],
      'application/json',
      -- 關閉思考模式：Supabase authenticated 逾時 8 秒，思考模式太慢
      jsonb_build_object('model', 'qwen/qwen3.8-27b', 'temperature', 0.3, 'max_tokens', max_tokens,
                         'reasoning_effort', 'none', 'reasoning_format', 'hidden', 'messages', messages)::text
    )::extensions.http_request);
  exception when others then
    return jsonb_build_object('error', 'AI 服務連線失敗：' || sqlerrm);
  end;
  if r.status <> 200 then
    return jsonb_build_object('error', 'AI 服務回應 ' || r.status || '：' || left(coalesce(r.content, ''), 200));
  end if;
  body := r.content::jsonb;
  -- 保險：若仍回傳 <think>…</think> 就去掉
  return jsonb_build_object('text', btrim(regexp_replace(coalesce(body #>> '{choices,0,message,content}', ''), '<think>.*?</think>', '', 'gs')),
                            'model', body->>'model');
end $$;
revoke all on function public._groq_chat(jsonb, int, int) from public, anon, authenticated;

create or replace function public._ai_quota(uid uuid)
returns text
language plpgsql security definer set search_path = public as $$
declare n int;
begin
  if uid is null then return '請先登入'; end if;
  perform pg_advisory_xact_lock(hashtext('ai:' || uid::text));
  select count(*) into n from ai_usage where user_id = uid and used_at > now() - interval '1 hour';
  if n >= 30 then return '本小時 AI 次數已達上限（30 次）'; end if;
  insert into ai_usage(user_id) values (uid);
  return null;
end $$;
revoke all on function public._ai_quota(uuid) from public, anon, authenticated;

/* AI 白話解說（進出場頁） */
create or replace function public.ai_explain(prompt text)
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare q text := _ai_quota(auth.uid());
begin
  if q is not null then return jsonb_build_object('error', q); end if;
  return _groq_chat(jsonb_build_array(
    jsonb_build_object('role', 'system', 'content',
      '你是台股投資助理。用繁體中文、台灣用語，根據使用者提供的規則引擎結果，用白話解說進出場建議與理由。不得自行編造數字，只能引用提供的數據。最後提醒僅供參考。'),
    jsonb_build_object('role', 'user', 'content', left(prompt, 6000))), 800);
end $$;

/* AI 選股聊天：附上盤後掃描清單，AI 只從清單中挑選 */
create or replace function public.ai_pick(messages jsonb)
returns jsonb
language plpgsql security definer set search_path = public, extensions as $$
declare
  q text := _ai_quota(auth.uid());
  ctx text;
  hist jsonb;
  d text;
begin
  if q is not null then return jsonb_build_object('error', q); end if;
  select string_agg('【' || label || '】' || rows::text, E'\n' order by signal), max(data_date)
    into ctx, d from ai_screen;
  if ctx is null then return jsonb_build_object('error', '選股資料尚未產生，請稍後再試'); end if;
  -- 只保留最近 6 則對話，每則最多 1500 字
  select coalesce(jsonb_agg(jsonb_build_object('role', m->>'role', 'content', left(m->>'content', 1500)) order by ord), '[]')
    into hist
    from (select m, ord from jsonb_array_elements(coalesce(messages, '[]')) with ordinality as t(m, ord)
           where m->>'role' in ('user', 'assistant') order by ord desc limit 6) x;
  return _groq_chat(
    jsonb_build_array(jsonb_build_object('role', 'system', 'content',
      '你是台灣股市 AI 選股助理，用繁體中文、台灣用語。規則：' ||
      '1. 只能從下面「盤後選股清單」挑股票，數字照抄清單，不可編造。' ||
      '2. 依使用者需求選最相關的清單，推薦 5～8 檔，每檔一行：代號 名稱｜理由（引用清單數字）。' ||
      '3. 清單沒有符合的就直說，並建議可以改問哪一類。4. 最後一行提醒僅供參考、非投資建議。' ||
      E'\n資料日：' || coalesce(d, '--') || E'\n盤後選股清單（JSON）：\n' || ctx))
    || hist, 1200, 6500);
end $$;

revoke all on function public.ai_explain(text) from public;
revoke all on function public.ai_pick(jsonb) from public;
grant execute on function public.ai_explain(text) to authenticated;
grant execute on function public.ai_pick(jsonb) to authenticated;

/* 備援排程：台北 00:30、08:30（每日更新 workflow 跑完也會主動呼叫一次） */
do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname = 'ai-screen-refresh';
  perform cron.schedule('ai-screen-refresh', '30 0,16 * * *', 'select public.ai_screen_refresh()');
end $$;
select public.ai_screen_refresh();

notify pgrst, 'reload schema';
