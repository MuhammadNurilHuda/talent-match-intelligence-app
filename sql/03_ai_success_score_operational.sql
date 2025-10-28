-- ============================================================
-- File: 03_ai_success_score_operational.sql
-- Purpose:
--   Operational view for Step 3 (Dashboard & App Layer)
--   Adds job_vacancy_id for parameterized access
--   Includes Strengths baseline patch and [0,1] clamping
-- Depends on:
--   mart.ai_success_score (from Step 2)
--   mart.v_employees_org
--   core.strengths
-- ============================================================

create or replace view mart.ai_success_score_operational as
with
-- ------------------------------------------------------------
-- 1. Benchmark references
-- ------------------------------------------------------------
bench_ids as (
  select tb.job_vacancy_id, unnest(tb.selected_talent_ids)::text as employee_id
  from config.talent_benchmarks tb
),
-- ------------------------------------------------------------
-- 2. Strengths transformation
-- ------------------------------------------------------------
strengths_long as (
  select s.employee_id,
         s.theme::text as theme
  from core.strengths s
),
bench_strengths as (
  select b.job_vacancy_id,
         sl.theme,
         count(*)::float / nullif(count(*) over (partition by b.job_vacancy_id),0) as share
  from strengths_long sl
  join bench_ids b on b.employee_id = sl.employee_id
  group by b.job_vacancy_id, sl.theme
),
baseline_strengths as (
  select job_vacancy_id, theme, 1.0::float as baseline_score
  from bench_strengths
  where share >= 0.5
),
user_strengths as (
  select employee_id, theme as tv_name, 1.0::float as user_score
  from strengths_long
),
strengths_match as (
  select
    b.job_vacancy_id,
    u.employee_id,
    'Strengths'::text as tgv_name,
    u.tv_name,
    coalesce(bs.baseline_score, 0.0)::float as baseline_score,
    u.user_score,
    case when bs.theme is not null then 1.0 else 0.0 end as tv_match_rate
  from user_strengths u
  cross join (select distinct job_vacancy_id from bench_ids) b
  left join baseline_strengths bs
    on bs.job_vacancy_id = b.job_vacancy_id
   and bs.theme = u.tv_name
),
-- ------------------------------------------------------------
-- 3. Other pillars (import from ai_success_score + clamp)
-- ------------------------------------------------------------
others as (
  select
    (select job_vacancy_id from config.talent_benchmarks order by created_at desc limit 1) as job_vacancy_id,
    a.employee_id, a.directorate, a.role, a.grade,
    a.tgv_name, a.tv_name,
    coalesce(a.baseline_score, 0.0)::float as baseline_score,
    coalesce(a.user_score, 0.0)::float as user_score,
    least(greatest(coalesce(a.tv_match_rate, 0.0), 0.0), 1.0) as tv_match_rate
  from mart.ai_success_score a
  where a.tgv_name <> 'Strengths'
),
-- ------------------------------------------------------------
-- 4. Unified dataset
-- ------------------------------------------------------------
unified as (
  select
    o.job_vacancy_id, o.employee_id, o.directorate, o.role, o.grade,
    o.tgv_name, o.tv_name, o.baseline_score, o.user_score, o.tv_match_rate
  from others o
  union all
  select
    s.job_vacancy_id, s.employee_id, v.directorate, v.position, v.grade,
    s.tgv_name, s.tv_name,
    s.baseline_score, s.user_score,
    least(greatest(s.tv_match_rate, 0.0), 1.0) as tv_match_rate
  from strengths_match s
  join mart.v_employees_org v using (employee_id)
),
-- ------------------------------------------------------------
-- 5. TGV-level aggregation
-- ------------------------------------------------------------
tgv as (
  select
    job_vacancy_id, employee_id, directorate, role, grade, tgv_name,
    avg(tv_match_rate) as tgv_match_rate
  from unified
  group by job_vacancy_id, employee_id, directorate, role, grade, tgv_name
),
-- ------------------------------------------------------------
-- 6. Final score calculation (Success Formula v2)
-- ------------------------------------------------------------
final as (
  select
    t1.job_vacancy_id, t1.employee_id, t1.directorate, t1.role, t1.grade,
    0.50*coalesce(max(case when tgv_name='Competency'   then tgv_match_rate end), 0)
  + 0.15*coalesce(max(case when tgv_name='Psychometric' then tgv_match_rate end), 0)
  + 0.25*coalesce(max(case when tgv_name='Strengths'    then tgv_match_rate end), 0)
  + 0.10*coalesce(max(case when tgv_name='Context'      then tgv_match_rate end), 0)
  as final_match_rate
  from tgv t1
  group by t1.job_vacancy_id, t1.employee_id, t1.directorate, t1.role, t1.grade
)
-- ------------------------------------------------------------
-- 7. Output view (operational)
-- ------------------------------------------------------------
select
  u.job_vacancy_id, u.employee_id, u.directorate, u.role, u.grade,
  u.tgv_name, u.tv_name,
  u.baseline_score, u.user_score, u.tv_match_rate,
  t.tgv_match_rate,
  f.final_match_rate
from unified u
join tgv   t on t.job_vacancy_id = u.job_vacancy_id and t.employee_id = u.employee_id and t.tgv_name = u.tgv_name
join final f on f.job_vacancy_id = u.job_vacancy_id and f.employee_id = u.employee_id;