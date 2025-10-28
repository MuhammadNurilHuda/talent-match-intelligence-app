# app.py
import os, json, streamlit as st, pandas as pd
from utils.data_fetch import (
    parse_ids, upsert_benchmark, fetch_leaderboard,
    fetch_candidate_tgv, fetch_candidate_tv,
    fetch_distribution, fetch_fairness
)
from utils.charts import radar_tgv, bars_tv, hist_distribution
from utils.ai_narrator import generate_job_profile

st.set_page_config(page_title="Talent Match Dashboard", layout="wide")

# -------------------- Sidebar: Vacancy Setup --------------------
st.sidebar.title("Vacancy Setup")
with st.sidebar.form("vacancy_form"):
    role_name    = st.text_input("Role name", placeholder="Data Analyst")
    job_level    = st.text_input("Job level", os.environ.get("DEFAULT_JOB_LEVEL", "Middle"))
    role_purpose = st.text_area("Role purpose", height=80)
    ids_raw      = st.text_area("Selected benchmark employee IDs (comma/line separated)")
    submit_btn   = st.form_submit_button("Generate / Update Benchmark")

if submit_btn:
    ids = parse_ids(ids_raw)
    if not role_name or not ids:
        st.sidebar.error("Role name & Benchmark IDs are required.")
    else:
        weights = json.loads(os.environ.get(
            "DEFAULT_WEIGHTS_JSON",
            '{"tgv":{"Competency":0.5,"Psychometric":0.15,"Strengths":0.25,"Context":0.1}}'
        ))
        jid = upsert_benchmark(role_name, job_level, role_purpose, ids, weights)
        st.sidebar.success(f"Benchmark saved. job_vacancy_id = {jid}")
        st.session_state["jid"] = jid

st.title("AI Talent App & Dashboard")

tab1, tab2, tab3, tab4 = st.tabs(["AI Job Profile", "Ranked Talent List", "Candidate Profile", "Insights"])

# -------------------- Tab 1: AI Job Profile --------------------
with tab1:
    st.subheader("AI-Generated Job Profile")
    if role_name:
        with st.spinner("Generating profile via OpenRouter..."):
            txt = generate_job_profile(role_name, job_level, role_purpose)
        st.markdown(txt)
        st.caption("Note: This AI section clarifies the role definition for analysis; it is not intended for public job postings.")
    else:
        st.info("Fill the Vacancy Setup on the left to generate an automatic job profile.")

# -------------------- Tab 2: Ranked Talent List --------------------
with tab2:
    st.subheader("Ranked Talent List")
    df = fetch_leaderboard(limit=200)
    if df.empty:
        st.warning("No data found in view mart.ai_success_score.")
    else:
        st.dataframe(df, use_container_width=True)
        st.caption("Pick an employee_id and paste it into the 'Candidate Profile' tab to view details.")

# -------------------- Tab 3: Candidate Profile --------------------
with tab3:
    st.subheader("Candidate Profile (Explain 'Why')")
    emp_id = st.text_input("Employee ID", placeholder="EMP100358")
    # Sensitivity control for gaps (does not change scoring logic)
    gap_threshold = st.slider("Gap threshold (tv_match_rate)", 0.50, 0.95, 0.70, 0.05)
    if emp_id:
        tgv = fetch_candidate_tgv(emp_id)
        if tgv.empty:
            st.warning("Candidate not found in the view.")
        else:
            c1, c2 = st.columns([1.2, 1])
            with c1:
                st.plotly_chart(radar_tgv(tgv, "TGV Match (0–1)"), use_container_width=True)

            # Choose TGV for variable-level bars
            pick_tgv = st.selectbox("Choose a TGV to show detailed TVs:", sorted(tgv["tgv_name"].unique().tolist()))
            tv = fetch_candidate_tv(emp_id, pick_tgv)

            # Robust typing (does not change logic—only ensures proper display)
            for col in ["baseline_score", "user_score", "tv_match_rate"]:
                if col in tv.columns:
                    tv[col] = pd.to_numeric(tv[col], errors="coerce")

            with c2:
                st.plotly_chart(bars_tv(tv, f"{pick_tgv} — TV Match"), use_container_width=True)

            # Gaps table (pure visualization; scoring logic unchanged)
            tv_disp = tv.copy()
            tv_disp[["baseline_score", "user_score", "tv_match_rate"]] = tv_disp[
                ["baseline_score", "user_score", "tv_match_rate"]
            ].fillna(0.0)

            gaps = tv_disp[tv_disp["tv_match_rate"] < gap_threshold].sort_values("tv_match_rate")

            st.markdown(f"**Gaps (tv_match_rate < {gap_threshold:.2f}):**")
            if gaps.empty:
                st.info(
                    "No gaps under the current threshold. Possible reasons: "
                    "the candidate is part of the benchmark, baselines for some TVs are missing, "
                    "or all variables meet/exceed the selected threshold. Try increasing the threshold (e.g., 0.85–0.90)."
                )
            else:
                st.dataframe(gaps, use_container_width=True, height=220)

# -------------------- Tab 4: Insights --------------------
with tab4:
    st.subheader("Insights & Fairness Glance")
    dist = fetch_distribution()
    if not dist.empty:
        st.plotly_chart(hist_distribution(dist), use_container_width=True)
        st.markdown(
            f"- N candidates: **{len(dist)}**  •  Mean: **{dist['final_match_rate'].mean():.3f}**  "
            f"•  P75: **{dist['final_match_rate'].quantile(0.75):.3f}**"
        )
    fair = fetch_fairness()
    if not fair.empty:
        st.markdown("**Average Final Match by Grade / Education / Major**")
        st.dataframe(fair, use_container_width=True, height=320)
        st.caption("Manually flag if a group deviates by more than ±0.10 from the overall mean.")