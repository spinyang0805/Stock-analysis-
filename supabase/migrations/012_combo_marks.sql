-- 組合拳「我有買」標記：使用者自己記的實際買／賣價，跟紙上追蹤分開，用來比對推薦準不準。可重複執行。
--   一筆紙上部位 = (stock_id, signal_date)；RLS 只能讀寫自己的列，不影響持股／損益頁
create table if not exists public.combo_marks (
  user_id     uuid not null default auth.uid() references auth.users(id) on delete cascade,
  stock_id    text not null,
  signal_date text not null,                    -- YYYYMMDD，對應 combo_signals.json 的 signal_date
  buy_date    date not null,
  buy_price   numeric not null check (buy_price > 0),
  sell_date   date,
  sell_price  numeric check (sell_price is null or sell_price > 0),
  updated_at  timestamptz not null default now(),
  primary key (user_id, stock_id, signal_date),
  check ((sell_date is null) = (sell_price is null)),
  check (sell_date is null or sell_date >= buy_date)
);
alter table public.combo_marks enable row level security;
revoke all on table public.combo_marks from anon;
grant select, insert, update, delete on table public.combo_marks to authenticated;
drop policy if exists "own combo marks" on public.combo_marks;
create policy "own combo marks" on public.combo_marks
  for all to authenticated using (user_id = auth.uid()) with check (user_id = auth.uid());
