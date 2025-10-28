# utils/data_fetch.py
from typing import List, Optional, Tuple
from uuid import uuid4
import json
import pandas as pd
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from .db import get_conn

def parse_ids(raw: str) -> List[int]:
    if not raw:
        return []
    # dukung "1, 2,3" atau baris baru
    parts = [p.strip() for p in raw.replace("\n", ",").split(",")]
    return [int(p) for p in parts if p.isdigit()]

def upsert_benchmark(role_name: str, job_level: str, role_purpose: str,
                     selected_ids: List[int], weights_json: dict) -> str:
    """
    Insert benchmark baru ke config.talent_benchmarks dan kembalikan job_vacancy_id.
    Asumsi: table sudah ada. Jika ingin 'single-active', handle di DB (mis. trigger/flag).
    """
    job_vacancy_id = str(uuid4())
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                insert into config.talent_benchmarks
                (job_vacancy_id, role_name, job_level, role_purpose,
                 selected_talent_ids, weights_config)
                values (%s, %s, %s, %s, %s, %s)
                returning job_vacancy_id
            """, (job_vacancy_id, role_name, job_level, role_purpose,
                  selected_ids, Jsonb(weights_json)))
            conn.commit()
            return cur.fetchone()["job_vacancy_id"]

def fetch_leaderboard(limit: int = 200) -> pd.DataFrame:
    sql = """
    select 
           a.employee_id,
           e.fullname,
           a.directorate,      -- ambil dari ai_success_score
           a.role,             -- ambil dari ai_success_score (bukan e.role)
           a.grade,            -- ambil dari ai_success_score
           max(a.final_match_rate) as final_match_rate,
           max(case when a.tgv_name='Competency'   then a.tgv_match_rate end) as tgv_comp,
           max(case when a.tgv_name='Psychometric' then a.tgv_match_rate end) as tgv_psy,
           max(case when a.tgv_name='Strengths'    then a.tgv_match_rate end) as tgv_str,
           max(case when a.tgv_name='Context'      then a.tgv_match_rate end) as tgv_ctx
    from mart.ai_success_score a
    join mart.v_employees_org e using (employee_id)
    group by a.employee_id, e.fullname, a.directorate, a.role, a.grade
    order by final_match_rate desc
    limit %s;
    """
    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=(limit,))
    return df

def fetch_candidate_tgv(emp_id: str) -> pd.DataFrame:
    sql = """
    select tgv_name, max(tgv_match_rate) as tgv_match_rate
    from mart.ai_success_score
    where employee_id = %s
    group by tgv_name
    order by tgv_name;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(emp_id,))

def fetch_candidate_tv(emp_id: str, tgv: str) -> pd.DataFrame:
    sql = """
    select tv_name, baseline_score, user_score, tv_match_rate
    from mart.ai_success_score
    where employee_id = %s and tgv_name = %s
    order by tv_name;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(emp_id, tgv))

def fetch_distribution() -> pd.DataFrame:
    sql = """
    select distinct employee_id,
           max(final_match_rate) over (partition by employee_id) as final_match_rate
    from mart.ai_success_score;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)
    
def fetch_fairness() -> pd.DataFrame:
    sql = """
    with base as (
      select a.employee_id,
             max(a.final_match_rate) as final_match_rate
      from mart.ai_success_score a
      group by a.employee_id
    )
    select e.grade, e.education, e.major,
           avg(b.final_match_rate) as avg_match, count(*) as n
    from base b
    join mart.v_employees_org e on e.employee_id = b.employee_id
    group by e.grade, e.education, e.major
    order by avg_match desc;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)