import streamlit as st
import pandas as pd
import numpy as np 

from data_loader import load_streamlit_data

st.title('Watchlist')
st.text('Stocks currently being monitored for significant price movement.')

# Loading the data

watchlist_df,_=load_streamlit_data()

# Building the summary statistics
total_watchlist = len(watchlist_df)
total_skyrocket=(watchlist_df['watchlist_status']=='SKYROCKET').sum()
total_drop=(watchlist_df['watchlist_status']=='DROP').sum()
total_normal=(watchlist_df['watchlist_status']=='NORMAL').sum()

# Icons

icons = {
    "Total Stocks" : "inventory_2",
    "SKYROCKET" :"trending_up",
    "DROP":"trending_down",
    "NORMAL":"remove_circle_outline"
}

# building the summary layout
#Create 4 horizontal container to put streamlie elements into
col1, col2, col3, col4= st.columns(4,
                                   #vertical_alignment="top",
                                   gap="medium",
                                   border=True)

#Create the metrics inside the columns
with col1:
    st.markdown(f":material/{icons['Total Stocks']}:")
    st.metric(
        label="Total Stocks", 
        value=total_watchlist
        )
    st.caption("Stocks tracked")

with col2:
    st.markdown(f":material/{icons['SKYROCKET']}:")
    st.metric(
        label= "SKYROCKET", 
        value= total_skyrocket
        )
    st.caption("Strong positive movement")

with col3:
    st.markdown(f":material/{icons['DROP']}:")
    st.metric(
        label="DROP", 
        value=total_drop
        )
    st.caption("Significant decline")

with col4:
    st.markdown(f":material/{icons['NORMAL']}:")
    st.metric(
        label='NORMAL',
        value=total_normal)
    st.caption("Within normal range")

st.divider()


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
                "90D Change (High → Low)",
                "90D Change (Low → High)",
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

    if sort_order == "90D Change (High → Low)":
        filtered_df = filtered_df.sort_values(
            "pct_change",
            ascending=False 
            )
    else:
        filtered_df = filtered_df.sort_values(
            "pct_change",
            ascending=True)

#Display Table
    display_df= filtered_df[
        [
        "ticker",
        "name",
        "current_adjclose",
        "pct_change",
        "watchlist_status"
    ]
    ].rename(columns={
        "ticker":"Ticker",
        "name":"Company",
        "current_adjclose":"Current price",
        "pct_change": "90D Change",
        "watchlist_status":"Status",
    })

    def style_status(value):
        if value == "SKYROCKET":
            return "background-color: #DCFCE7; color:#166534; font-weight: 600;"
        elif value == "DROP":
            return "background-color: #FEE2E2; color:#991B1B; font-weight: 600;"

        elif value == "NORMAL":
            return "background-color: #F3F4F6; color:#F3F4F6; font-weigh:600"

        return ""

    styled_df = display_df.style.map(
        style_status,
        subset=['Status']
    )

    display_df["90D Change"] = (
        display_df["90D Change"]
        .map(lambda x: f"{x:.2f}%")
    )

# Table

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Current price": st.column_config.NumberColumn(
                "Current price",
                format="$%.2f"
            )
        }
        )


    
