from datetime import date

import pandas as pd
import streamlit as st

from integrations.bigquery import query_to_dataframe

BUDGET_TABLE_ID = "carmax-ph.liveagent_staging.budget_technique_test"
TICKETS_TABLE_ID = "carmax-ph.liveagent.tickets"
DISPLAY_COLUMNS = [
    "ticket_id",
    "budget_technique_used",
    "continued_after_budget",
    "budget_nonstarter",
    "first_budget_ask_at",
    "window_start",
    "nonstarter_reason",
]


@st.cache_data(ttl=300)
def _load_budget_data() -> pd.DataFrame:
    query = f"""
        SELECT
            ticket_id,
            budget_technique_used,
            continued_after_budget,
            budget_nonstarter,
            first_budget_ask_at,
            window_start,
            nonstarter_reason
        FROM `{BUDGET_TABLE_ID}`
    """
    return query_to_dataframe(query)


@st.cache_data(ttl=900)
def _load_ticket_date_bounds() -> tuple[date | None, date | None]:
    query = f"""
        SELECT
            MIN(DATE(date_created)) AS min_date,
            MAX(DATE(date_created)) AS max_date
        FROM `{TICKETS_TABLE_ID}`
        WHERE date_created IS NOT NULL
    """
    df = query_to_dataframe(query)
    if df.empty:
        return None, None
    return df.loc[0, "min_date"], df.loc[0, "max_date"]


@st.cache_data(ttl=900)
def _load_monthly_total_tickets(month_start: date) -> int:
    month_start_iso = month_start.isoformat()
    query = f"""
        SELECT COUNT(DISTINCT CAST(id AS STRING)) AS total_tickets
        FROM `{TICKETS_TABLE_ID}`
        WHERE DATE(date_created) >= DATE('{month_start_iso}')
          AND DATE(date_created) < DATE_ADD(DATE('{month_start_iso}'), INTERVAL 1 MONTH)
    """
    df = query_to_dataframe(query)
    if df.empty:
        return 0
    return int(df.loc[0, "total_tickets"] or 0)


def _prepare_budget_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared["first_budget_ask_at"] = pd.to_datetime(prepared["first_budget_ask_at"], errors="coerce")
    prepared["window_start"] = pd.to_datetime(prepared["window_start"], errors="coerce")
    prepared["event_at"] = prepared["first_budget_ask_at"].combine_first(prepared["window_start"])
    prepared["event_date"] = prepared["event_at"].dt.date
    return prepared


def _rate_theme(rate_pct: float, good_high: bool) -> dict:
    if good_high:
        if rate_pct >= 60:
            return {"bg": "linear-gradient(135deg, #dcfce7, #bbf7d0)", "border": "#22c55e"}
        if rate_pct >= 30:
            return {"bg": "linear-gradient(135deg, #fef9c3, #fde68a)", "border": "#f59e0b"}
        return {"bg": "linear-gradient(135deg, #fee2e2, #fecaca)", "border": "#ef4444"}

    if rate_pct >= 30:
        return {"bg": "linear-gradient(135deg, #fee2e2, #fecaca)", "border": "#ef4444"}
    if rate_pct >= 15:
        return {"bg": "linear-gradient(135deg, #fef9c3, #fde68a)", "border": "#f59e0b"}
    return {"bg": "linear-gradient(135deg, #dcfce7, #bbf7d0)", "border": "#22c55e"}


def _render_metric_cards(cards: list[dict]) -> None:
    st.markdown(
        """
        <style>
        .bt-card {
            border-radius: 14px;
            padding: 14px 16px;
            min-height: 126px;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.10);
        }
        .bt-title {
            font-size: 0.84rem;
            font-weight: 700;
            color: #111827;
            margin-bottom: 8px;
        }
        .bt-value {
            font-size: 1.9rem;
            font-weight: 800;
            line-height: 1.1;
            color: #0f172a;
            margin-bottom: 4px;
        }
        .bt-sub {
            font-size: 0.81rem;
            color: #374151;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    first_row = st.columns(4)
    second_row = st.columns(3)

    for col, item in zip(first_row + second_row, cards):
        col.markdown(
            f"""
            <div class="bt-card" style="background: {item['bg']}; border: 1px solid {item['border']};">
                <div class="bt-title">{item['title']}</div>
                <div class="bt-value">{item['value']}</div>
                <div class="bt-sub">{item['sub']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


st.write("# Budget Technique")

try:
    budget_df = _prepare_budget_dataframe(_load_budget_data())
except Exception as exc:
    st.error(f"Failed to load budget-technique data: {exc}")
    st.stop()

if budget_df.empty:
    st.info("No budget-technique data found.")
    st.stop()

try:
    picker_min, picker_max = _load_ticket_date_bounds()
except Exception as exc:
    st.warning(f"Could not load ticket date bounds; falling back to budget-technique dates. ({exc})")
    picker_min, picker_max = None, None

if picker_min is None or picker_max is None:
    valid_event_dates = budget_df["event_date"].dropna()
    if valid_event_dates.empty:
        st.warning("No valid dates available for filtering.")
        st.stop()
    picker_min = valid_event_dates.min()
    picker_max = valid_event_dates.max()

default_start = picker_max.replace(day=1)
if default_start < picker_min:
    default_start = picker_min

default_end = picker_max

c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Start date", value=default_start, min_value=picker_min, max_value=picker_max)
with c2:
    end_date = st.date_input("End date", value=default_end, min_value=picker_min, max_value=picker_max)

if start_date > end_date:
    st.error("Start date cannot be after end date.")
    st.stop()

filtered_df = budget_df[
    budget_df["event_date"].ge(start_date) & budget_df["event_date"].le(end_date)
].copy()

selected_month = pd.Period(start_date, freq="M")
month_start = selected_month.start_time.date()
month_label = selected_month.strftime("%B %Y")
monthly_total_tickets = _load_monthly_total_tickets(month_start)

if pd.Period(end_date, freq="M") != selected_month:
    st.warning("Monthly metrics use the month of the selected start date.")

budget_used_mask = filtered_df["budget_technique_used"].fillna(False).astype(bool)
continued_mask = filtered_df["continued_after_budget"].fillna(False).astype(bool)
nonstarter_budget_mask = filtered_df["budget_nonstarter"].fillna(False).astype(bool)

budget_tickets = int(filtered_df.loc[budget_used_mask, "ticket_id"].nunique(dropna=True))
continued_tickets = int(filtered_df.loc[budget_used_mask & continued_mask, "ticket_id"].nunique(dropna=True))
budget_nonstarter_tickets = int(
    filtered_df.loc[budget_used_mask & nonstarter_budget_mask, "ticket_id"].nunique(dropna=True)
)

budget_usage_rate = (budget_tickets / monthly_total_tickets * 100) if monthly_total_tickets else 0.0
budget_continuation_rate = (continued_tickets / budget_tickets * 100) if budget_tickets else 0.0
budget_nonstarter_rate = (budget_nonstarter_tickets / budget_tickets * 100) if budget_tickets else 0.0
continuation_rate_vs_all = (continued_tickets / monthly_total_tickets * 100) if monthly_total_tickets else 0.0

usage_theme = {"bg": "linear-gradient(135deg, #dbeafe, #bfdbfe)", "border": "#60a5fa"}
continuation_theme = _rate_theme(budget_continuation_rate, good_high=True)
nonstarter_theme = _rate_theme(budget_nonstarter_rate, good_high=False)
continuation_all_theme = _rate_theme(continuation_rate_vs_all, good_high=True)

cards = [
    {
        "title": "Total number of tickets (month)",
        "value": f"{monthly_total_tickets:,}",
        "sub": f"Month: {month_label}",
        **usage_theme,
    },
    {
        "title": "Tickets with budget technique used",
        "value": f"{budget_tickets:,}",
        "sub": f"{budget_usage_rate:.1f}% of monthly tickets",
        **usage_theme,
    },
    {
        "title": "Budget technique usage rate",
        "value": f"{budget_usage_rate:.1f}%",
        "sub": f"{budget_tickets:,} / {monthly_total_tickets:,} tickets",
        **usage_theme,
    },
    {
        "title": "Budget technique continuation rate",
        "value": f"{budget_continuation_rate:.1f}%",
        "sub": f"Among budget-technique tickets",
        **continuation_theme,
    },
    {
        "title": "Nonstarter rate among budget-technique tickets",
        "value": f"{budget_nonstarter_rate:.1f}%",
        "sub": f"{budget_nonstarter_tickets:,} budget nonstarters",
        **nonstarter_theme,
    },
    {
        "title": "Continued because of budget technique",
        "value": f"{continued_tickets:,}",
        "sub": f"Conversations/tickets continued",
        **continuation_theme,
    },
    {
        "title": "Continuation rate of the budget technique",
        "value": f"{continuation_rate_vs_all:.1f}%",
        "sub": "Continued tickets vs monthly total",
        **continuation_all_theme,
    },
]

_render_metric_cards(cards)

show_df = filtered_df.copy()
if "event_at" in show_df.columns:
    show_df = show_df.sort_values(by="event_at", ascending=False)

available_columns = [col for col in DISPLAY_COLUMNS if col in show_df.columns]
st.dataframe(show_df[available_columns], use_container_width=True)
