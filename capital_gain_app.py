import streamlit as st
from style import apply_custom_style
from data_loader import get_available_dates 
from components import render_date_pill_selector

st.set_page_config(page_title="Stock Dividend Monitor", layout="wide")
apply_custom_style()

st.sidebar.markdown("""
<div style="display: flex; align-items: center; gap: 10px; padding: 0.5rem 0 1.5rem 0;">
    <span style="font-size: 1.8rem;">📈</span>
    <div>
        <div style="font-size: 1.1rem; font-weight: 700; color: #E5E7EB; line-height: 1.2;">Stock Dividend Monitor</div>
        <div style="font-size: 0.75rem; color: #6B7280;">Track. Monitor. Profit.</div>
    </div>
</div>
""", unsafe_allow_html=True)


dashboard_page = st.Page(
    "pages/dashboard.py",
    title="Dashboard",
    icon="📊",
    default=True
)

watchlist_page = st.Page(
    "pages/watchlist.py",
    title="Watchlist",
    icon="⭐"
)


pg = st.navigation([dashboard_page, watchlist_page])
pg.run()


