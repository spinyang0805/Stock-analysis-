-- 私人資料：使用者的持股建議（股數、成本）與推薦追蹤狀態（含是否持有），只給後端排程讀寫。
-- 不放在公開的 GitHub Release；網站前端（anon / authenticated）無法讀取。
create table if not exists public.private_store (
  key        text primary key,          -- tracked_picks / holdings:YYYYMMDD / holdings_line:YYYYMMDD
  body       text not null,
  updated_at timestamptz not null default now()
);
alter table public.private_store enable row level security;
revoke all on table public.private_store from anon, authenticated;

do $$
begin
  perform cron.unschedule(jobid) from cron.job where jobname = 'private-store-cleanup';
  perform cron.schedule('private-store-cleanup', '50 6 * * *',
    $c$delete from public.private_store where key ~ '^holdings(_line)?:' and updated_at < now() - interval '60 days'$c$);
end $$;
