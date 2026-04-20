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
    "nonstarter_reason",
    "first_client_message_at",
    "first_agent_reply_at",
    "last_client_message_at",
]


@st.cache_data(ttl=300)
def _load_nonstarters_data() -> pd.DataFrame:
    query = f"""
        SELECT
            ticket_id,
            nonstarter_reason,
            first_client_message_at,
            first_agent_reply_at,
            last_client_message_at
        FROM `{NONSTARTERS_TABLE_ID}`
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


def _prepare_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    for col in ["first_client_message_at", "first_agent_reply_at", "last_client_message_at"]:
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


def _render_kpi_cards(total_tickets: int, nonstarters_count: int, month_label: str) -> None:
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
            <div class="kpi-title">Total number of tickets</div>
            <div class="kpi-value">{total_tickets:,}</div>
            <div class="kpi-sub">Month: {month_label}</div>
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
            <div class="kpi-sub">{nonstarter_rate:.1f}% of monthly tickets</div>
            <div class="kpi-tag">{theme['label']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.write("# Nonstarters")

try:
    df = _prepare_datetime_columns(_load_nonstarters_data())
except Exception as exc:
    st.error(f"Failed to load nonstarters data: {exc}")
    st.stop()

if df.empty:
    st.info("No data found.")
    st.stop()

monthly_total_tickets = 0
monthly_label = "N/A"

# Date picker bounds are based on liveagent.tickets so users can navigate months
# even when nonstarters has sparse rows.
try:
    picker_min, picker_max = _load_ticket_date_bounds()
except Exception as exc:
    st.warning(f"Could not load ticket date bounds; falling back to nonstarters dates. ({exc})")
    picker_min, picker_max = None, None

if picker_min is None or picker_max is None:
    if df["first_client_message_at"].notna().any():
        picker_min = df["first_client_message_at"].min().date()
        picker_max = df["first_client_message_at"].max().date()
    else:
        st.warning("No valid dates available for filtering.")
        filtered_df = df.copy()
        nonstarters_mask = filtered_df["nonstarter_reason"].fillna("").astype(str).str.strip().ne("")
        nonstarters_count = int(filtered_df.loc[nonstarters_mask, "ticket_id"].nunique(dropna=True))
        _render_kpi_cards(total_tickets=0, nonstarters_count=nonstarters_count, month_label=monthly_label)
        available_columns = [col for col in DISPLAY_COLUMNS if col in filtered_df.columns]
        st.dataframe(filtered_df[available_columns], use_container_width=True)
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

if df["first_client_message_at"].notna().any():
    date_mask = (
        df["first_client_message_at"].dt.date.ge(start_date)
        & df["first_client_message_at"].dt.date.le(end_date)
    )
    filtered_df = df[date_mask].copy()
else:
    filtered_df = df.iloc[0:0].copy()

selected_month = pd.Period(start_date, freq="M")
month_start = selected_month.start_time.date()
monthly_total_tickets = _load_monthly_total_tickets(month_start)
monthly_label = selected_month.strftime("%B %Y")

if pd.Period(end_date, freq="M") != selected_month:
    st.warning("Total number of tickets uses the month of the selected start date.")

filtered_df = filtered_df.sort_values(by="first_client_message_at", ascending=False)

nonstarters_mask = filtered_df["nonstarter_reason"].fillna("").astype(str).str.strip().ne("")
nonstarters_count = int(filtered_df.loc[nonstarters_mask, "ticket_id"].nunique(dropna=True))

_render_kpi_cards(total_tickets=monthly_total_tickets, nonstarters_count=nonstarters_count, month_label=monthly_label)

available_columns = [col for col in DISPLAY_COLUMNS if col in filtered_df.columns]
st.dataframe(filtered_df[available_columns], use_container_width=True)

