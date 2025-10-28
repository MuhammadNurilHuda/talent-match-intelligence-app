# app.py
import os, json, streamlit as st, pandas as pd
from utils.data_fetch import (parse_ids, upsert_benchmark, fetch_leaderboard,
                              fetch_candidate_tgv, fetch_candidate_tv,
                              fetch_distribution, fetch_fairness)
from utils.charts import radar_tgv, bars_tv, hist_distribution
from utils.ai_narrator import generate_job_profile

st.set_page_config(page_title="Talent Match Dashboard", layout="wide")

st.sidebar.title("Vacancy Setup")
with st.sidebar.form("vacancy_form"):
    role_name   = st.text_input("Role name", placeholder="Data Analyst")
    job_level   = st.text_input("Job level", os.environ.get("DEFAULT_JOB_LEVEL","Middle"))
    role_purpose= st.text_area("Role purpose", height=80)
    ids_raw     = st.text_area("Selected benchmark employee IDs (comma/line separated)")
    submit_btn  = st.form_submit_button("Generate / Update Benchmark")

if submit_btn:
    ids = parse_ids(ids_raw)
    if not role_name or not ids:
        st.sidebar.error("Role name & Benchmark IDs wajib diisi.")
    else:
        weights = json.loads(os.environ.get("DEFAULT_WEIGHTS_JSON",
                     '{"tgv":{"Competency":0.5,"Psychometric":0.15,"Strengths":0.25,"Context":0.1}}'))
        jid = upsert_benchmark(role_name, job_level, role_purpose, ids, weights)
        st.sidebar.success(f"Benchmark saved. job_vacancy_id = {jid}")
        st.session_state["jid"] = jid

st.title("AI Talent App & Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["AI Job Profile", "Ranked Talent List", "Candidate Profile", "Insights"])

with tab1:
    st.subheader("AI-Generated Job Profile")
    if role_name:
        with st.spinner("Generating profile via OpenRouter..."):
            txt = generate_job_profile(role_name, job_level, role_purpose)
        st.markdown(txt)
    else:
        st.info("Isi Vacancy di sidebar untuk menghasilkan profil pekerjaan otomatis.")

with tab2:
    st.subheader("Ranked Talent List")
    df = fetch_leaderboard(limit=200)
    if df.empty:
        st.warning("Belum ada data pada view mart.ai_success_score.")
    else:
        st.dataframe(df, use_container_width=True)
        st.caption("Klik satu employee_id dan salin ke tab 'Candidate Profile' untuk melihat detail.")

with tab3:
    st.subheader("Candidate Profile")
    emp_id = st.text_input("Employee ID", placeholder="EMP100358")
    if emp_id:
        tgv = fetch_candidate_tgv(emp_id)
        if tgv.empty:
            st.warning("Kandidat tidak ditemukan di view.")
        else:
            c1, c2 = st.columns([1.2, 1])
            with c1: st.plotly_chart(radar_tgv(tgv, "TGV Match (0–1)"), use_container_width=True)
            # pilih TGV untuk bar TVs
            pick_tgv = st.selectbox("Pilih TGV untuk detail TV:", tgv["tgv_name"].tolist())
            tv = fetch_candidate_tv(emp_id, pick_tgv)
            with c2: st.plotly_chart(bars_tv(tv, f"{pick_tgv} — TV Match"), use_container_width=True)
            # gaps
            gaps = tv[tv["tv_match_rate"] < 0.7].sort_values("tv_match_rate")
            st.markdown("**Gaps (tv_match_rate < 0.7):**")
            st.dataframe(gaps, use_container_width=True, height=200)

with tab4:
    st.subheader("Insights & Fairness Glance")
    dist = fetch_distribution()
    if not dist.empty:
        st.plotly_chart(hist_distribution(dist), use_container_width=True)
        st.markdown(f"- N candidates: **{len(dist)}**  •  Mean: **{dist['final_match_rate'].mean():.3f}**  •  P75: **{dist['final_match_rate'].quantile(0.75):.3f}**")
    fair = fetch_fairness()
    if not fair.empty:
        st.markdown("**Average Final Match by Grade/Education/Major**")
        st.dataframe(fair, use_container_width=True, height=320)
        st.caption("Flag manual jika ada selisih grup > ±0.10 terhadap rata-rata keseluruhan.")