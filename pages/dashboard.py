import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px 

from data_loader import load_streamlit_data

def generate_header_title():
    """
    Display 'Dashboard' as the header & 'Overview of today's market and watchlist as text'
    """

    st.title('Dashboard')
    st.caption(f"Overview of Today's Market and Watchlist")

     

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
        st.metric(
            label='Total Stocks Monitored',
            value= stocks_monitored_count,
            width='stretch'
        )


    with col2:
        st.metric(
            label="Dividend Payers",
            value=dividend_paying_stocks,
            help="Number of stocks in the watchlist currently classified as dividend paying."

        )

    with col3:
        st.metric(
            label="Current Watchlist",
            value=current_watchlist
        )

    with col4:
        st.metric(
            label="Avg 90D Price Change",
            value=f"{average_price_movement:.2f}%"
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

    fig = px.bar(
        x=bucket_labels,
        y=bucket_counts.values,
        labels={
            "x": "Percentage Change",
            "y": "Number of Stocks"
        }
    )

    st.plotly_chart(fig)

    return

def gen_watchlist_preview(watchlist_df:pd.DataFrame):
    """"Display the watchlist preview table.
    """

    st.caption("Watchlist(5)")

    watchlist_df=watchlist_df.copy()

    watchlist_df=(watchlist_df.sort_values(
        'pct_change',
        ascending=False
    ).head(5))

    display_df=watchlist_df[[
        'ticker',
        'name',
        'current_adjclose',
        'pct_change',
        'watchlist_status'
    ]].rename(columns={
        'ticker': 'Ticker',
        'name':'Company',
        'current_adjclose':'Current Price',
        'pct_change': '90D Change',
        'watchlist_status': 'Status'
    })

    display_df['90D Change']=display_df['90D Change'].map(lambda x: f"{x:.2f}%" )
    
    return display_df

def gen_top_movers(watchlist_df:pd.DataFrame):
    """Display top gainers and top losers.
    """

    watchlist_col=watchlist_df[[
        'ticker',
        'name',
        'pct_change'
    ]]

    top_gainers= (watchlist_col.sort_values(
        'pct_change',
        ascending=False

    ).head(5))

    top_losers= (watchlist_col.sort_values(
        'pct_change',
        ascending=True
    ).head(5))


    show_gainers = st.toggle("Top Movers", value=True)

    if show_gainers:
        st.caption('Top Gainers')
        st.dataframe(top_gainers, hide_index=True)

    else:
        st.caption('Top Losers')
        st.dataframe(top_losers, hide_index=True)
    

    #return show_top_movers



#Loading data
watchlist_df, dividend_df = load_streamlit_data()
generate_header_title()
generate_summary_cards(watchlist_df,dividend_df)
gen_price_change_dist(watchlist_df)
preview= gen_watchlist_preview(watchlist_df)
top=gen_top_movers(watchlist_df)

st.dataframe(
    top,
    hide_index=True
)






