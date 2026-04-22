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


NONSTARTERS_TABLE_ID = _required_env("NONSTARTERS_TABLE_ID")
TICKETS_TABLE_ID = _required_env("TICKETS_TABLE_ID")
DISPLAY_COLUMNS = [
    "ticket_id",
    "ticket_status",
    "ticket_date_created",
    "nonstarter_reason",
    "first_client_message_at",
    "first_agent_reply_at",
    "last_client_message_at",
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
def _load_nonstarters_in_range(start_date: date, end_date: date) -> pd.DataFrame:
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    query = f"""
        SELECT
            CAST(t.id AS STRING) AS ticket_id,
            t.status AS ticket_status,
            t.date_created AS ticket_date_created,
            n.nonstarter_reason,
            n.first_client_message_at,
            n.first_agent_reply_at,
            n.last_client_message_at
        FROM `{TICKETS_TABLE_ID}` t
        INNER JOIN `{NONSTARTERS_TABLE_ID}` n
            ON CAST(t.id AS STRING) = CAST(n.ticket_id AS STRING)
        WHERE DATE(t.date_created) >= DATE('{start_iso}')
          AND DATE(t.date_created) <= DATE('{end_iso}')
    """
    return query_to_dataframe(query)


def _prepare_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    for col in [
        "ticket_date_created",
        "first_client_message_at",
        "first_agent_reply_at",
        "last_client_message_at",
    ]:
        if col in prepared.columns:
            prepared[col] = pd.to_datetime(prepared[col], errors="coerce")
    return prepared


def _get_nonstarter_theme(nonstarter_rate_pct: float) -> dict:
    if nonstarter_rate_pct >= 30:
        return {
            "bg": "linear-gradient(135deg, #fee2e2, #fecaca)",
            "border": "#ef4444",
            "label": "Very high nonstarters",
        }
    if nonstarter_rate_pct >= 15:
        return {
            "bg": "linear-gradient(135deg, #fef9c3, #fde68a)",
            "border": "#f59e0b",
            "label": "Moderate nonstarters",
        }
    return {
        "bg": "linear-gradient(135deg, #dcfce7, #bbf7d0)",
        "border": "#22c55e",
        "label": "Low nonstarters",
    }


def _render_kpi_cards(total_tickets: int, nonstarters_count: int, period_label: str) -> None:
    nonstarter_rate = (nonstarters_count / total_tickets * 100) if total_tickets else 0.0
    theme = _get_nonstarter_theme(nonstarter_rate)

    st.markdown(
        """
        <style>
        .kpi-card {
            border-radius: 14px;
            padding: 16px 18px;
            min-height: 130px;
            box-shadow: 0 6px 20px rgba(2, 6, 23, 0.09);
        }
        .kpi-title {
            font-size: 0.86rem;
            font-weight: 700;
            color: #111827;
            margin-bottom: 6px;
        }
        .kpi-value {
            font-size: 2rem;
            font-weight: 800;
            color: #0f172a;
            line-height: 1.1;
            margin-bottom: 4px;
        }
        .kpi-sub {
            font-size: 0.83rem;
            color: #374151;
        }
        .kpi-tag {
            display: inline-block;
            margin-top: 8px;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            color: #111827;
            background: rgba(255,255,255,0.65);
            border: 1px solid rgba(17,24,39,0.12);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns(2)
    c1.markdown(
        f"""
        <div class="kpi-card" style="background: linear-gradient(135deg, #dbeafe, #bfdbfe); border: 1px solid #60a5fa;">
            <div class="kpi-title">Total tickets created</div>
            <div class="kpi-value">{total_tickets:,}</div>
            <div class="kpi-sub">Range: {period_label}</div>
            <div class="kpi-sub">Nonstarter rate: {nonstarter_rate:.1f}%</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c2.markdown(
        f"""
        <div class="kpi-card" style="background: {theme['bg']}; border: 1px solid {theme['border']};">
            <div class="kpi-title">Number of nonstarters</div>
            <div class="kpi-value">{nonstarters_count:,}</div>
            <div class="kpi-sub">{nonstarter_rate:.1f}% of selected tickets</div>
            <div class="kpi-tag">{theme['label']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.write("# Nonstarters")

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
    filtered_df = _prepare_datetime_columns(_load_nonstarters_in_range(start_date, end_date))
    total_tickets = _load_total_tickets_in_range(start_date, end_date)
except Exception as exc:
    st.error(f"Failed to load nonstarters for selected range: {exc}")
    st.stop()

period_label = f"{start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}"
nonstarters_count = int(filtered_df["ticket_id"].nunique(dropna=True)) if "ticket_id" in filtered_df.columns else 0

if "ticket_date_created" in filtered_df.columns:
    filtered_df = filtered_df.sort_values(by="ticket_date_created", ascending=False)

_render_kpi_cards(total_tickets=total_tickets, nonstarters_count=nonstarters_count, period_label=period_label)

if filtered_df.empty:
    st.info("No nonstarters found for the selected ticket-created date range.")

available_columns = [col for col in DISPLAY_COLUMNS if col in filtered_df.columns]
st.dataframe(filtered_df[available_columns], use_container_width=True)
