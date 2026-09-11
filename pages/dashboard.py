import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px 
import datetime


from data_loader import load_streamlit_data
from style import apply_custom_style


st.set_page_config(page_title="Dashboard", layout="wide")
apply_custom_style()

def generate_header_title():
    """
    Display 'Dashboard' as the header & 'Overview of today's market and watchlist as text'
    """

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem;">
        <div>
            <h1 style="margin-bottom: 0; font-size: 2rem;">📈 Dashboard</h1>
            <p style="color: #9CA3AF; margin-top: 0.2rem;">Overview of today's market and watchlist</p>
        </div>
        <div style="text-align: right; color: #9CA3AF; font-size: 0.9rem;">
            🕐 Last updated: {now}
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_metric_card(label, value, subtext, color, icon):
    """
    Renders a single metric card as HTML.
    """

    st.markdown(f"""
    <div style="
        background-color: #111827;
        border: 1px solid #1F2937;
        border-radius: 12px;
        padding: 1.2rem;
        height: 100%;
    ">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <span style="color: {color}; font-size: 0.9rem; font-weight: 600;">{label}</span>
            <span style="font-size: 1.3rem;">{icon}</span>
        </div>
        <div style="font-size: 1.8rem; font-weight: 700; color: #E5E7EB; margin-top: 0.4rem;">
            {value}
        </div>
        <div style="color: #6B7280; font-size: 0.8rem; margin-top: 0.3rem;">
            {subtext}
        </div>
    </div>
    """, unsafe_allow_html=True)

     

def generate_summary_cards(watchlist_df:pd.DataFrame, dividend_df:pd.DataFrame):
    """
    Creation of the four summary cards.
    """

    #Generation of the summary statistics
            
        
    stocks_monitored_count= len(watchlist_df)
    dividend_paying_stocks= (dividend_df['dividend_status']=='dividend_payer').sum()
    dividend_pct= (dividend_paying_stocks / stocks_monitored_count)* 100
    current_skyrocket=(watchlist_df['watchlist_status']=='SKYROCKET').sum()
    current_drop=(watchlist_df['watchlist_status']=='DROP').sum()
    current_watchlist= current_skyrocket + current_drop
    average_price_movement=watchlist_df['pct_change'].mean()
    

    #Generate summary layouts with 4 horizontal containers

    col1, col2, col3, col4= st.columns(4,
                                       gap="medium",
                                       border=True)

    #Generate the metrics to put inside the columns
    with col1:
        render_metric_card(
            "Total Stocks Monitored", stocks_monitored_count,
            "All monitored stocks", "#3B82F6", "📊"

        )


    with col2:
        render_metric_card(
            "Dividend Payers", dividend_paying_stocks,
            f"{dividend_pct:.1f}% of total", "#22C55E", "💰"
        )
    

    with col3:
        render_metric_card(
            "Current Watchlist", current_watchlist,
            f"{current_skyrocket} skyrocket / {current_drop} drop", "#F97316", "⚡"

        )

    with col4:
        render_metric_card(
            "Avg 90D Price Change", f"{average_price_movement:.2f}%",
            "All stocks", "#A855F7","📈"
        )

    return watchlist_df, dividend_df

def gen_price_change_dist(watchlist_df:pd.DataFrame):
    """
    Calculates the price change distribution.
    Displays the price change histogram"""

    #Choose the buckets

    bucket_edges= [-float("inf"),
                   -30,
                   -20,
                   -10,
                   0,
                   10,
                   20,
                   30,
                   50,
                   float("inf")]

    watchlist_df['pct_change_bucket'] = pd.cut(
        watchlist_df['pct_change'],
        bins=bucket_edges
    )

    bucket_counts = (
        watchlist_df['pct_change_bucket']
        .value_counts()
        .sort_index()
    )

    bucket_labels = [
    "< -30%",
    "-30% to -20%",
    "-20% to -10%",
    "-10% to 0%",
    "0% to 10%",
    "10% to 20%",
    "20% to 30%",
    "30% to 50%",
    "> 50%"]

    bar_colors = [
        "#EF4444", "#F97316", "#F59E0B", "#FBBF24",
        "#84CC16", "#22C55E","#10B981", "#3B82F6","#A855F7"
    ]

    st.subheader("Price Change Distribution (90 Days)")

    fig = px.bar(
        x=bucket_labels,
        y=bucket_counts.values,
        labels={
            "x": "90 Day Price Change (%)",
            "y": "Number  of  Stocks"
        },
        text=bucket_counts.values
    )

    fig.update_traces(
        marker=dict(color=bar_colors),
        textposition="outside"
    )

    fig.update_layout(
        plot_bgcolor= "rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#E5E7EB",
        xaxis=dict(showgrid=False, title_standoff=25),
        yaxis=dict(showgrid=True,gridcolor="#1F2937"),
        margin=dict(t=20, b=20, l=20, r=20)
    )

    st.plotly_chart(fig, use_container_width=True )

    return

def get_status_badge(status):
    """
    Returns an HTML pill badge for a given status string.
    """

    colors = {
        "SKYROCKET": ("#22C55E", "#052e16"),
        "DROP": ("#EF4444", "#450a0a"),
        "NORMAL": ("#3B82F6", "#172554"),
    }
    text_color, bg_color = colors.get(status, ("#9CA3AF", "#1F2937"))
    return f'<span style="background-color:{bg_color}; color:{text_color}; padding:3px 10px; border-radius:999px; font-size:0.75rem; font-weight:600;">{status}</span>'
  

def render_watchlist_table(watchlist_df: pd.DataFrame):
    st.caption("Watchlist (5)")
    df = watchlist_df.copy().sort_values('pct_change', ascending=False).head(5)

    rows_html = ""
    for _, row in df.iterrows():
        badge = get_status_badge(row['watchlist_status'])
        change_color = '#22C55E' if row['pct_change'] >= 0 else '#EF4444'
        rows_html += (
            f'<tr>'
            f'<td style="padding:8px; color:#E5E7EB;">{row["ticker"]}</td>'
            f'<td style="padding:8px; color:#9CA3AF;">{row["name"]}</td>'
            f'<td style="padding:8px; color:#E5E7EB;">${row["current_adjclose"]:.2f}</td>'
            f'<td style="padding:8px; color:{change_color};">{row["pct_change"]:.2f}%</td>'
            f'<td style="padding:8px;">{badge}</td>'
            f'</tr>'
        )

    table_html = (
        '<table style="width:100%; border-collapse:collapse; font-size:0.85rem;">'
        '<thead><tr style="color:#6B7280; text-align:left; border-bottom:1px solid #1F2937;">'
        '<th style="padding:8px;">Ticker</th>'
        '<th style="padding:8px;">Company</th>'
        '<th style="padding:8px;">Price</th>'
        '<th style="padding:8px;">90D Change</th>'
        '<th style="padding:8px;">Status</th>'
        '</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table>'
    )

    st.markdown(table_html, unsafe_allow_html=True)  

def gen_top_movers(watchlist_df: pd.DataFrame):
    """Display top gainers and top losers as a styled HTML table."""

    watchlist_col = watchlist_df[['ticker', 'name', 'pct_change']]

    top_gainers = watchlist_col.sort_values('pct_change', ascending=False).head(5)
    top_losers = watchlist_col.sort_values('pct_change', ascending=True).head(5)

    def build_table(df):
        rows_html = ""
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            change_color = '#22C55E' if row['pct_change'] >= 0 else '#EF4444'
            rows_html += (
                f'<tr>'
                f'<td style="padding:8px; color:#6B7280;">{i}</td>'
                f'<td style="padding:8px; color:#E5E7EB;">{row["ticker"]}</td>'
                f'<td style="padding:8px; color:#9CA3AF;">{row["name"]}</td>'
                f'<td style="padding:8px; color:{change_color}; text-align:right;">{row["pct_change"]:.2f}%</td>'
                f'</tr>'
            )
        return (
            '<table style="width:100%; border-collapse:collapse; font-size:0.85rem;">'
            '<thead><tr style="color:#6B7280; text-align:left; border-bottom:1px solid #1F2937;">'
            '<th style="padding:8px;">#</th>'
            '<th style="padding:8px;">Ticker</th>'
            '<th style="padding:8px;">Company</th>'
            '<th style="padding:8px; text-align:right;">90D Change</th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            '</table>'
        )

    tab1, tab2 = st.tabs(["Top Gainers", "Top Losers"])
    with tab1:
        st.markdown(build_table(top_gainers), unsafe_allow_html=True)
    with tab2:
        st.markdown(build_table(top_losers), unsafe_allow_html=True)



#Loading data
watchlist_df, dividend_df = load_streamlit_data()
generate_header_title()
generate_summary_cards(watchlist_df,dividend_df)
gen_price_change_dist(watchlist_df)

col_left, col_right = st.columns(2, gap='medium')

with col_left:
    with st.container(border=True):
        render_watchlist_table(watchlist_df)
    

with col_right:
    with st.container(border=True):
        gen_top_movers(watchlist_df)






