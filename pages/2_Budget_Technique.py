from datetime import date
import os

import pandas as pd
import streamlit as st

from integrations.bigquery import query_to_dataframe


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BUDGET_TABLE_ID = _required_env("BUDGET_TABLE_ID")
TICKETS_TABLE_ID = _required_env("TICKETS_TABLE_ID")
DISPLAY_COLUMNS = [
    "ticket_id",
    "ticket_status",
    "ticket_date_created",
    "budget_technique_used",
    "continued_after_budget",
    "budget_nonstarter",
    "first_budget_ask_at",
    "window_start",
    "nonstarter_reason",
]


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
def _load_total_tickets_in_range(start_date: date, end_date: date) -> int:
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    query = f"""
        SELECT COUNT(DISTINCT CAST(id AS STRING)) AS total_tickets
        FROM `{TICKETS_TABLE_ID}`
        WHERE DATE(date_created) >= DATE('{start_iso}')
          AND DATE(date_created) <= DATE('{end_iso}')
    """
    df = query_to_dataframe(query)
    if df.empty:
        return 0
    return int(df.loc[0, "total_tickets"] or 0)


@st.cache_data(ttl=300)
def _load_budget_in_range(start_date: date, end_date: date) -> pd.DataFrame:
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    query = f"""
        SELECT
            CAST(t.id AS STRING) AS ticket_id,
            t.status AS ticket_status,
            t.date_created AS ticket_date_created,
            b.budget_technique_used,
            b.continued_after_budget,
            b.budget_nonstarter,
            b.first_budget_ask_at,
            b.window_start,
            b.nonstarter_reason
        FROM `{TICKETS_TABLE_ID}` t
        INNER JOIN `{BUDGET_TABLE_ID}` b
            ON CAST(t.id AS STRING) = CAST(b.ticket_id AS STRING)
        WHERE DATE(t.date_created) >= DATE('{start_iso}')
          AND DATE(t.date_created) <= DATE('{end_iso}')
    """
    return query_to_dataframe(query)


def _prepare_budget_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    for col in ["ticket_date_created", "first_budget_ask_at", "window_start"]:
        if col in prepared.columns:
            prepared[col] = pd.to_datetime(prepared[col], errors="coerce")
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
    picker_min, picker_max = _load_ticket_date_bounds()
except Exception as exc:
    st.error(f"Failed to load ticket date bounds: {exc}")
    st.stop()

if picker_min is None or picker_max is None:
    st.warning("No valid ticket creation dates available for filtering.")
    st.stop()

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

try:
    filtered_df = _prepare_budget_dataframe(_load_budget_in_range(start_date, end_date))
    total_tickets = _load_total_tickets_in_range(start_date, end_date)
except Exception as exc:
    st.error(f"Failed to load budget-technique data for selected range: {exc}")
    st.stop()

budget_used_mask = filtered_df["budget_technique_used"].fillna(False).astype(bool)
continued_mask = filtered_df["continued_after_budget"].fillna(False).astype(bool)
nonstarter_budget_mask = filtered_df["budget_nonstarter"].fillna(False).astype(bool)

budget_tickets = int(filtered_df.loc[budget_used_mask, "ticket_id"].nunique(dropna=True))
continued_tickets = int(filtered_df.loc[budget_used_mask & continued_mask, "ticket_id"].nunique(dropna=True))
budget_nonstarter_tickets = int(
    filtered_df.loc[budget_used_mask & nonstarter_budget_mask, "ticket_id"].nunique(dropna=True)
)

budget_usage_rate = (budget_tickets / total_tickets * 100) if total_tickets else 0.0
budget_continuation_rate = (continued_tickets / budget_tickets * 100) if budget_tickets else 0.0
budget_nonstarter_rate = (budget_nonstarter_tickets / budget_tickets * 100) if budget_tickets else 0.0
continuation_rate_vs_all = (continued_tickets / total_tickets * 100) if total_tickets else 0.0

usage_theme = {"bg": "linear-gradient(135deg, #dbeafe, #bfdbfe)", "border": "#60a5fa"}
continuation_theme = _rate_theme(budget_continuation_rate, good_high=True)
nonstarter_theme = _rate_theme(budget_nonstarter_rate, good_high=False)
continuation_all_theme = _rate_theme(continuation_rate_vs_all, good_high=True)

period_label = f"{start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}"
cards = [
    {
        "title": "Total number of tickets (range)",
        "value": f"{total_tickets:,}",
        "sub": f"Range: {period_label}",
        **usage_theme,
    },
    {
        "title": "Tickets with budget technique used",
        "value": f"{budget_tickets:,}",
        "sub": f"{budget_usage_rate:.1f}% of selected tickets",
        **usage_theme,
    },
    {
        "title": "Budget technique usage rate",
        "value": f"{budget_usage_rate:.1f}%",
        "sub": f"{budget_tickets:,} / {total_tickets:,} tickets",
        **usage_theme,
    },
    {
        "title": "Budget technique continuation rate",
        "value": f"{budget_continuation_rate:.1f}%",
        "sub": "Among budget-technique tickets",
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
        "sub": "Conversations/tickets continued",
        **continuation_theme,
    },
    {
        "title": "Continuation rate of the budget technique",
        "value": f"{continuation_rate_vs_all:.1f}%",
        "sub": "Continued tickets vs selected total",
        **continuation_all_theme,
    },
]

_render_metric_cards(cards)

show_df = filtered_df.copy()
if "ticket_date_created" in show_df.columns:
    show_df = show_df.sort_values(by="ticket_date_created", ascending=False)

if show_df.empty:
    st.info("No budget-technique records found for the selected ticket-created date range.")

available_columns = [col for col in DISPLAY_COLUMNS if col in show_df.columns]
st.dataframe(show_df[available_columns], use_container_width=True)
