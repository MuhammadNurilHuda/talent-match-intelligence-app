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
    # "1, 2,3" or new line
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

def fetch_leaderboard(limit: int = 200, job_vacancy_id: Optional[str] = None) -> pd.DataFrame:
    sql = """
    with jid AS (
        select %s::text as job_vacancy_id
    ),
    jid_final AS (
        select coalesce(j.job_vacancy_id,
                        (select job_vacancy_id
                           from config.talent_benchmarks
                           order by created_at desc
                           limit 1)) as job_vacancy_id
        from jid j
    )
    select 
        a.employee_id,
        e.fullname,
        a.directorate,
        a.role,
        a.grade,
        max(a.final_match_rate) as final_match_rate,
        max(case when a.tgv_name='Competency'   then a.tgv_match_rate end) as tgv_comp,
        max(case when a.tgv_name='Psychometric' then a.tgv_match_rate end) as tgv_psy,
        max(case when a.tgv_name='Strengths'    then a.tgv_match_rate end) as tgv_str,
        max(case when a.tgv_name='Context'      then a.tgv_match_rate end) as tgv_ctx
    from mart.ai_success_score_operational a
    join jid_final jf on jf.job_vacancy_id = a.job_vacancy_id
    join mart.v_employees_org e using (employee_id)
    group by a.employee_id, e.fullname, a.directorate, a.role, a.grade
    order by final_match_rate desc
    limit %s;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(job_vacancy_id, limit))

def fetch_candidate_tgv(employee_id: str, job_vacancy_id: Optional[str] = None) -> pd.DataFrame:
    sql = """
    with jid AS (select %s::text as job_vacancy_id),
    jid_final AS (
        select coalesce(j.job_vacancy_id,
                        (select job_vacancy_id
                           from config.talent_benchmarks
                           order by created_at desc
                           limit 1)) as job_vacancy_id
        from jid j
    )
    select
      t.employee_id, t.directorate, t.role, t.grade,
      t.tgv_name, t.tgv_match_rate
    from (
      select
        job_vacancy_id, employee_id, directorate, role, grade, tgv_name,
        avg(tv_match_rate) as tgv_match_rate
      from mart.ai_success_score_operational
      where employee_id = %s
      group by job_vacancy_id, employee_id, directorate, role, grade, tgv_name
    ) t
    join jid_final jf on jf.job_vacancy_id = t.job_vacancy_id
    order by tgv_name;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(job_vacancy_id, employee_id))

def fetch_candidate_tv(employee_id: str, tgv_name: str, job_vacancy_id: Optional[str] = None) -> pd.DataFrame:
    sql = """
    with jid AS (select %s::text as job_vacancy_id),
    jid_final AS (
        select coalesce(j.job_vacancy_id,
                        (select job_vacancy_id
                           from config.talent_benchmarks
                           order by created_at desc
                           limit 1)) as job_vacancy_id
        from jid j
    )
    select
      a.tv_name, a.baseline_score, a.user_score, a.tv_match_rate
    from mart.ai_success_score_operational a
    join jid_final jf on jf.job_vacancy_id = a.job_vacancy_id
    where a.employee_id = %s
      and a.tgv_name = %s
    order by a.tv_name;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(job_vacancy_id, employee_id, tgv_name))

def fetch_distribution(job_vacancy_id: Optional[str] = None) -> pd.DataFrame:
    sql = """
    with jid AS (select %s::text as job_vacancy_id),
    jid_final AS (
        select coalesce(j.job_vacancy_id,
                        (select job_vacancy_id
                           from config.talent_benchmarks
                           order by created_at desc
                           limit 1)) as job_vacancy_id
        from jid j
    )
    select distinct a.employee_id, a.final_match_rate
    from mart.ai_success_score_operational a
    join jid_final jf on jf.job_vacancy_id = a.job_vacancy_id;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(job_vacancy_id,))
    
def fetch_fairness(job_vacancy_id: Optional[str] = None) -> pd.DataFrame:
    sql = """
    with jid AS (select %s::text as job_vacancy_id),
    jid_final AS (
        select coalesce(j.job_vacancy_id,
                        (select job_vacancy_id
                           from config.talent_benchmarks
                           order by created_at desc
                           limit 1)) as job_vacancy_id
        from jid j
    )
    select
      a.grade, e.education, e.major,
      avg(a.final_match_rate) as avg_match
    from mart.ai_success_score_operational a
    join jid_final jf on jf.job_vacancy_id = a.job_vacancy_id
    join mart.v_employees_org e using (employee_id)
    group by a.grade, e.education, e.major
    order by a.grade, e.education, e.major;
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=(job_vacancy_id,))