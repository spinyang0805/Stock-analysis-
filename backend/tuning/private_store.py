"""private_store.py — tiny key/value store in Supabase (public.private_store, migration 007) for data that must
never be published: the user's holdings advice and the tracking state (which picks the user holds).

  get(key) -> str | None        put(key, body)        available() -> bool
"""
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)


def _load_env():
    path = os.path.join(BACKEND, ".env")
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#") and "=" in ln:
                    k, _, v = ln.partition("=")
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _connect():
    import psycopg2
    _load_env()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


def available():
    try:
        c = _connect()
        c.close()
        return True
    except Exception:
        return False


def get(key):
    c = _connect()
    try:
        with c.cursor() as cur:
            cur.execute("SELECT body FROM public.private_store WHERE key = %s", (key,))
            r = cur.fetchone()
        c.rollback()
        return r[0] if r else None
    finally:
        c.close()


def put(key, body):
    c = _connect()
    try:
        with c.cursor() as cur:
            cur.execute("""INSERT INTO public.private_store (key, body, updated_at) VALUES (%s, %s, NOW())
                           ON CONFLICT (key) DO UPDATE SET body = EXCLUDED.body, updated_at = NOW()""", (key, body))
        c.commit()
    finally:
        c.close()
