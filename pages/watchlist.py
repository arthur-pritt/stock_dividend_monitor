import streamlit as st
import pandas as pd
import numpy as np 
import math

from data_loader import load_streamlit_data
from style import apply_custom_style
from components import render_metric_card, get_status_badge


title_col, button_col = st.columns([5, 1])
with title_col:
    st.title('Watchlist')
    st.text('Stocks currently being monitored for significant price movement.')
with button_col:
    st.write("")  # spacer to push button down, roughly aligning with title
    st.write("")
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# Loading the data

watchlist_df,_=load_streamlit_data()

# Building the summary statistics
total_watchlist = len(watchlist_df)
total_skyrocket=(watchlist_df['watchlist_status']=='SKYROCKET').sum()
total_drop=(watchlist_df['watchlist_status']=='DROP').sum()
total_normal=(watchlist_df['watchlist_status']=='NORMAL').sum()

#Create the metrics inside the columns

col1, col2, col3, col4 = st.columns(4, gap="medium")

with col1:
    render_metric_card("Total Watchlist", total_watchlist, "Stocks tracked", "#3B82F6", "📋")

with col2:
    render_metric_card("SKYROCKET", total_skyrocket, ">= +50% (90D Change)", "#22C55E", "🚀")

with col3:
    render_metric_card("DROP", total_drop, "<= -20% (90D Change)", "#EF4444", "📉")

with col4:
    render_metric_card("NORMAL", total_normal, "Other Stocks", "#F59E0B", "➖")


st.write("")
st.write("")
with st.container(border= True):

    #st.subheader("watchlist")

    latest_date = pd.to_datetime(
        watchlist_df['latest_date']
    ).max()

    st.caption(
        f"Date as of {latest_date:%B %d,%Y}"
    )
    search_col, status_col, sort_col=st.columns([2,1,1])

    with search_col:
        search = st.text_input(
            "Search",
            placeholder="Search ticker or Company..."
        )

    with status_col:
        selected_status = st.selectbox(
            "Status",
            [
                "All",
                "SKYROCKET",
                "NORMAL",
                "DROP"
            ]
        )

    with sort_col:
        sort_order = st.selectbox(
            "Sort by",
            [
                "90D Change (High to Low)",
                "90D Change (Low to High)",
            ]

        )


# Filtering

    filtered_df = watchlist_df.copy()

    if selected_status !="All":
        filtered_df = filtered_df[
            filtered_df['watchlist_status']== selected_status
            ]

# Search

    if search:
        search_mask=(
            filtered_df['ticker'].str.contains(search,case=False, na=False) |
            filtered_df['name'].str.contains(search, case=False, na=False))

        filtered_df =filtered_df[search_mask]
# Sort

    if sort_order == "90D Change (High to Low)":
        filtered_df = filtered_df.sort_values(
            "pct_change",
            ascending=False 
            )
    else:
        filtered_df = filtered_df.sort_values(
            "pct_change",
            ascending=True)

    # Reset to page 1 whenever the filtered result set changes
    filter_signature = (search, selected_status, sort_order, len(filtered_df))
    if st.session_state.get("last_filter_signature") != filter_signature:
        st.session_state.page_number = 1
        st.session_state.last_filter_signature = filter_signature

    def get_page_slice(df: pd.DataFrame, page_size=10):
        """Just computes which rows to show for the current page. No UI here."""
        total_rows = len(df)
        total_pages = max(1, math.ceil(total_rows / page_size))

        if "page_number" not in st.session_state:
            st.session_state.page_number = 1

        st.session_state.page_number = min(st.session_state.page_number, total_pages)
        current = st.session_state.page_number

        start_row = (current - 1) * page_size
        end_row = min(start_row + page_size, total_rows)

        return df.iloc[start_row:end_row], start_row, end_row, total_rows, total_pages, current

    def render_pagination_controls(start_row, end_row, total_rows, total_pages, current, max_buttons=5):
        """Draws the 'Showing X to Y' text + page number buttons."""
        left_col, right_col = st.columns([2, 3])

        with left_col:
            st.markdown(f"Showing {start_row + 1} to {end_row} of {total_rows}")

        with right_col:
            window_start = max(1, current - (max_buttons // 2))
            window_end = min(total_pages, window_start + max_buttons - 1)
            window_start = max(1, window_end - max_buttons + 1)

            page_numbers = list(range(window_start, window_end + 1))
            n_cols = len(page_numbers) + 2
            nav_cols = st.columns(n_cols)

            with nav_cols[0]:
                if st.button("<", disabled=current == 1, key="prev"):
                    st.session_state.page_number -= 1
                    st.rerun()

            for idx, page_num in enumerate(page_numbers):
                with nav_cols[idx + 1]:
                    btn_type = "primary" if page_num == current else "secondary"
                    if st.button(str(page_num), key=f"page_{page_num}", type=btn_type):
                        st.session_state.page_number = page_num
                        st.rerun()

            with nav_cols[-1]:
                if st.button(">", disabled=current == total_pages, key="next"):
                    st.session_state.page_number += 1
                    st.rerun()

    # --- Pagination happens on the raw filtered data first ---
    page_df, start_row, end_row, total_rows, total_pages, current = get_page_slice(
        filtered_df, page_size=10)

    # --- Build the HTML table for just this page's rows ---
    rows_html = ""
    for _, row in page_df.iterrows():
        badge = get_status_badge(row['watchlist_status'])
        change_color = '#22C55E' if row['pct_change'] >= 0 else '#EF4444'
        sign = '+' if row['pct_change'] >= 0 else ''
        rows_html += (
            f'<tr>'
            f'<td style="padding:8px; color:#E5E7EB;">{row["ticker"]}</td>'
            f'<td style="padding:8px; color:#9CA3AF;">{row["name"]}</td>'
            f'<td style="padding:8px; color:#E5E7EB;">${row["current_adjclose"]:.2f}</td>'
            f'<td style="padding:8px; color:{change_color};">{sign}{row["pct_change"]:.2f}%</td>'
            f'<td style="padding:8px;">{badge}</td>'
            f'</tr>'
        )

    table_html = (
        '<table style="width:100%; border-collapse:collapse; font-size:0.85rem;">'
        '<thead><tr style="color:#6B7280; text-align:left; border-bottom:1px solid #1F2937;">'
        '<th style="padding:8px;">Ticker</th>'
        '<th style="padding:8px;">Company</th>'
        '<th style="padding:8px;">Current price</th>'
        '<th style="padding:8px;">90D Change</th>'
        '<th style="padding:8px;">Status</th>'
        '</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table>'
    )

    st.markdown(table_html, unsafe_allow_html=True)

# Controls now render AFTER the table
    render_pagination_controls(start_row, end_row, total_rows, total_pages, current)




    
