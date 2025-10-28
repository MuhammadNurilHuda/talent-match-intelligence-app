# utils/charts.py
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def radar_tgv(df_tgv: pd.DataFrame, title: str = "TGV Profile"):
    # df: tgv_name | tgv_match_rate
    categories = df_tgv["tgv_name"].tolist()
    values = df_tgv["tgv_match_rate"].tolist()
    values += values[:1]; categories += categories[:1]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=values, theta=categories, fill="toself", name="TGV"))
    fig.update_layout(title=title, polar=dict(radialaxis=dict(visible=True, range=[0,1])))
    return fig

def bars_tv(df_tv: pd.DataFrame, title: str = "TV Match"):
    # df: tv_name | tv_match_rate
    return px.bar(df_tv, x="tv_name", y="tv_match_rate", title=title, range_y=[0,1])

def hist_distribution(df: pd.DataFrame, title: str = "Final Match Distribution"):
    return px.histogram(df, x="final_match_rate", nbins=30, title=title, range_x=[0,1])