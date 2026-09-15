import streamlit as st

def render_date_pill_selector(available_dates):
    """
    Renders a compact pill-style date selector, meant to sit
    as its own row below the page header.
    Returns the selected date (or None for 'latest').
    """
    sorted_dates = sorted(available_dates, reverse=True)
    latest_date = sorted_dates[0]

    date_labels = [d.strftime("%b %d, %Y") for d in sorted_dates]
    date_lookup = {d.strftime("%b %d, %Y"): d for d in sorted_dates}

    chosen_label = st.selectbox(
        "Date",
        date_labels,
        label_visibility="collapsed",
        key="global_date_selector"
    )

    chosen_date = date_lookup[chosen_label]

    if chosen_label == latest_date:
        return None
    return chosen_date

def render_metric_card(label, value, subtext, color, icon):
    """
    Renders a single styled metric card as HTML.
    Shared across Dashboard, Watchlist, and other pages.
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


def get_status_badge(status):
    """Returns an HTML pill badge for a given status string."""
    colors = {
        "SKYROCKET": ("#22C55E", "#052e16"),
        "DROP": ("#EF4444", "#450a0a"),
        "NORMAL": ("#3B82F6", "#172554"),
    }
    text_color, bg_color = colors.get(status, ("#9CA3AF", "#1F2937"))
    return f'<span style="background-color:{bg_color}; color:{text_color}; padding:3px 10px; border-radius:999px; font-size:0.75rem; font-weight:600;">{status}</span>'