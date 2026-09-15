import streamlit as st

def apply_custom_style():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    #MainMenu, footer, header {visibility: hidden;}

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    [data-testid="stSidebar"] {
        background-color: #0B1120;
        border-right: 1px solid #1F2937;
    }

    [data-testid="stSidebarContent"] {
        display: flex;
        flex-direction: column;
    }
    [data-testid="stSidebarHeader"] {
        order: -2;
    }
    [data-testid="stSidebarUserContent"] {
        order: -1;
        padding-bottom: 0 !important;
        margin-bottom: 0 !important;
    }
    [data-testid="stSidebarUserContent"] > div:last-child {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    [data-testid="stSidebarNav"] {
        order: 2;
        margin-top: 0 !important;
        padding-top: 0.5rem !important;
    }

    [data-testid="stSidebarNav"] a {
        border-radius: 8px;
        padding: 8px 12px;
        margin: 2px 8px;
        color: #9CA3AF;
        font-weight: 500;
    }

    [data-testid="stSidebarNav"] a:hover {
        background-color: #1F2937;
        color: #E5E7EB;
    }

    [data-testid="stSidebarNav"] a[aria-current="page"] {
        background-color: #3B82F6;
        color: white;
    }

    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #111827;
        border: 1px solid #1F2937 !important;
        border-radius: 12px !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
    }

    [data-testid="stTabs"] [role="tablist"] {
        display: flex;
        width: 100%;
        gap: 12px;
        border-bottom: none !important;
    }
    [data-testid="stTab"] {
        flex: 1;
        display: flex !important;
        justify-content: center;
        background-color: #1F2937 !important;
        border-radius: 8px !important;
        padding: 8px 20px !important;
        color: #9CA3AF !important;
        border: none !important;
    }
    [data-testid="stTab"] p {
        color: inherit !important;
        margin: 0;
    }
    [data-testid="stTab"][aria-selected="true"][data-selected="true"] {
        color: white !important;
    }
    [data-testid="stTab"][aria-selected="true"][data-selected="true"] p {
        color: white !important;
    }
    [data-testid="stTab"]:first-of-type[aria-selected="true"][data-selected="true"] {
        background-color: #22C55E !important;
    }
    [data-testid="stTab"]:last-of-type[aria-selected="true"][data-selected="true"] {
        background-color: #EF4444 !important;
    }

    table tr {
        border-bottom: 1px solid #1F2937;
    }
    table tr:hover {
        background-color: #1a2332;
    }
    table tr:last-child {
        border-bottom: none;
    }

    .stButton button[kind="primary"] {
        background-color: #3B82F6 !important;
        border-color: #3B82F6 !important;
    }

    div[data-testid="stTextInput"] input {
        padding-left: 2.2rem !important;
        background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='%239CA3AF' viewBox='0 0 16 16'%3E%3Cpath d='M11.742 10.344a6.5 6.5 0 1 0-1.397 1.398h-.001q.044.06.098.115l3.85 3.85a1 1 0 0 0 1.415-1.414l-3.85-3.85a1 1 0 0 0-.115-.1zM12 6.5a5.5 5.5 0 1 1-11 0 5.5 5.5 0 0 1 11 0'/%3E%3C/svg%3E");
        background-repeat: no-repeat;
        background-position: 12px center;
    }

    hr {
        border: none;
        height: 1px;
        background-color: #1F2937;
        margin: 1.5rem 0;
    }

        div[data-testid="stSelectbox"] > div > div {
        background-color: #111827 !important;
        border: 1px solid #1F2937 !important;
        border-radius: 999px !important;
    }
    </style>
    """, unsafe_allow_html=True)