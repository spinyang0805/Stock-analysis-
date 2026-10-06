-- PostgREST upsert(on_conflict=user_id,import_hash) 需要非部分的唯一索引；
-- NULL 彼此不相等，手動輸入（import_hash 為 NULL）的列不受影響。
drop index if exists public.transactions_user_hash;
create unique index if not exists transactions_user_hash_full on public.transactions (user_id, import_hash);
