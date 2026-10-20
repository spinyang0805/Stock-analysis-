-- 美股（國泰複委託）交易、美股日收盤價、借貸追蹤設定。可重複執行（db-migrate 每次 push 全部重跑）。
--   us_transactions : 使用者自己的美股交易（對帳單匯入），RLS 只能讀寫自己的列
--   us_prices       : 持有過的美股代號日收盤價 + USDTWD 匯率，由 us-prices 排程（DATABASE_URL）寫入；
--                     只給登入者讀（代號本身會透露持股，不給 anon）
--   loan_tracker    : 借貸追蹤設定（貸款條件、銀行餘額），每位使用者一列，RLS 只能讀寫自己的

create table if not exists public.us_transactions (
  id          uuid primary key default gen_random_uuid(),
  user_id     uuid not null default auth.uid() references auth.users(id) on delete cascade,
  trade_date  date not null,
  ticker      text not null,
  name        text,
  side        text not null check (side in ('buy','sell','cash_dividend')),
  shares      numeric not null default 0,
  price       numeric not null default 0,      -- 成交價；除息時為每股配息
  fee         numeric not null default 0,      -- 手續費
  other_fee   numeric not null default 0,      -- 其他費用（賣出規費、股利預扣稅）
  gross       numeric,                          -- 成交金額
  amount      numeric,                          -- 應收/付金額（買進為負）
  fx          numeric not null default 1,
  currency    text not null default 'USD',
  import_hash text,
  created_at  timestamptz not null default now()
);
create unique index if not exists us_transactions_user_hash on public.us_transactions (user_id, import_hash);
create index if not exists us_transactions_user_date on public.us_transactions (user_id, trade_date);
alter table public.us_transactions enable row level security;
drop policy if exists "own us transactions" on public.us_transactions;
create policy "own us transactions" on public.us_transactions
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

create table if not exists public.us_prices (
  ticker     text not null,                     -- 美股代號；匯率用 'USDTWD'（1 美元 = close 台幣）
  date       date not null,
  close      numeric not null,
  updated_at timestamptz not null default now(),
  primary key (ticker, date)
);
alter table public.us_prices add column if not exists source text not null default 'yahoo';  -- bot = 臺灣銀行牌告即期買入
alter table public.us_prices enable row level security;
revoke all on table public.us_prices from anon;
grant select on table public.us_prices to authenticated;
drop policy if exists "read us prices" on public.us_prices;
create policy "read us prices" on public.us_prices for select to authenticated using (true);

create table if not exists public.loan_tracker (
  user_id    uuid primary key default auth.uid() references auth.users(id) on delete cascade,
  data       jsonb not null,
  updated_at timestamptz not null default now()
);
alter table public.loan_tracker enable row level security;
drop policy if exists "own loan tracker" on public.loan_tracker;
create policy "own loan tracker" on public.loan_tracker
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());
