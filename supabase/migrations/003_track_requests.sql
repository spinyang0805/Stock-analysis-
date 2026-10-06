-- 「加入每日追蹤」請求：前端登入者寫入，GitHub Actions（track-stocks.yml）每 20 分鐘處理
create table if not exists public.track_requests (
  code         text primary key check (code ~ '^[0-9A-Z]{4,6}$'),
  requested_by uuid default auth.uid() references auth.users(id) on delete set null,
  requested_at timestamptz not null default now(),
  status       text not null default 'pending' check (status in ('pending', 'done', 'invalid', 'error')),
  message      text,
  done_at      timestamptz
);
alter table public.track_requests enable row level security;

-- 任何人可查進度（只有代號與狀態，無個資）；登入者可新增；狀態只由後端（postgres 角色）更新
drop policy if exists "read track requests" on public.track_requests;
create policy "read track requests" on public.track_requests for select using (true);
drop policy if exists "request tracking" on public.track_requests;
create policy "request tracking" on public.track_requests for insert to authenticated
  with check (requested_by = auth.uid() and status = 'pending');

notify pgrst, 'reload schema';
