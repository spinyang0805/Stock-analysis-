import os
from psycopg2 import pool as pg_pool

DATABASE_URL = os.getenv("DATABASE_URL")
_pool = None

try:
    if DATABASE_URL:
        _pool = pg_pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
        print("Database pool initialized")
except Exception as e:
    print("Database pool init error:", e)

db = _pool  # kept for backwards-compat `if db is None` checks throughout codebase


def get_conn():
    if _pool is None:
        return None
    try:
        return _pool.getconn()
    except Exception as e:
        print("get_conn error:", e)
        return None


def return_conn(conn):
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass
