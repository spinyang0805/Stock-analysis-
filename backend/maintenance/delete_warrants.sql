-- PREVIEW ONLY - not executed. Deletes warrant (權證/牛熊證) rows listed in warrant_ids.txt.
-- Run from backend/maintenance with psql (autocommit ON, session-mode port 5432):
--   psql "$DIRECT_DATABASE_URL" -f delete_warrants.sql
-- Preferred way is: python maintenance/delete_warrants.py --apply  (same logic + safety re-checks).
\set ON_ERROR_STOP on
\set ids `paste -sd, warrant_ids.txt`

CREATE TEMP TABLE _w (id text PRIMARY KEY);
INSERT INTO _w SELECT unnest(string_to_array(:'ids', ','));

-- Safety: never touch codes the user owns, nor ETF / REIT / ETN / TDR prefixes.
DELETE FROM _w WHERE id IN (SELECT stock_id FROM transactions
                            UNION SELECT stock_id FROM watchlist
                            UNION SELECT code FROM track_requests
                            UNION SELECT stock_id FROM holding_settings);
DELETE FROM _w WHERE left(id, 2) IN ('00', '01', '02', '91') OR id !~ '^[0-9]{5}[0-9A-Z]$';

-- Batches of 200 ids, COMMIT after each batch.
DO $$
DECLARE
    ids   text[];
    batch text[];
    i     int;
BEGIN
    SELECT array_agg(id ORDER BY id) INTO ids FROM _w;
    FOR i IN 0 .. cardinality(ids) - 1 BY 200 LOOP
        batch := ids[i + 1 : i + 200];
        DELETE FROM stock_daily       WHERE stock_id = ANY(batch);
        DELETE FROM chip_daily        WHERE stock_id = ANY(batch);
        DELETE FROM analysis_cache    WHERE stock_id = ANY(batch);
        DELETE FROM fundamentals      WHERE stock_id = ANY(batch);
        DELETE FROM financials_yearly WHERE stock_id = ANY(batch);
        DELETE FROM heal_blacklist    WHERE stock_id = ANY(batch);
        DELETE FROM product_universe  WHERE code     = ANY(batch);
        COMMIT;
    END LOOP;
END $$;

-- Plain VACUUM only (no FULL: it needs temporary extra disk, DB is near the 500 MB quota).
VACUUM (ANALYZE) stock_daily;
VACUUM (ANALYZE) chip_daily;
VACUUM (ANALYZE) analysis_cache;
VACUUM (ANALYZE) fundamentals;
VACUUM (ANALYZE) financials_yearly;
VACUUM (ANALYZE) heal_blacklist;
VACUUM (ANALYZE) product_universe;
SELECT pg_size_pretty(pg_database_size(current_database()));
