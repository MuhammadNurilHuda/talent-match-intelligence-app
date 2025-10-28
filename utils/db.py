# utils/db.py
import os
import psycopg
from contextlib import contextmanager

def _get_dsn() -> str:
    # Prioritas: Streamlit secrets → env var
    try:
        import streamlit as st
        if "PG_CONN" in st.secrets:
            return st.secrets["PG_CONN"]
    except Exception:
        pass
    dsn = os.environ.get("PG_CONN")
    if not dsn:
        raise RuntimeError(
            "Missing PG_CONN. Set it in .streamlit/secrets.toml or environment.\n"
            "Example: postgres://postgres:<PASSWORD>@db.<PROJECT-REF>.supabase.co:5432/postgres?sslmode=require"
        )
    return dsn

@contextmanager
def get_conn():
    dsn = _get_dsn()
    with psycopg.connect(dsn, autocommit=False) as conn:
        yield conn