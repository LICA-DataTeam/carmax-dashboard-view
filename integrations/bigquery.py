import json
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


def _normalize_creds(creds: dict) -> dict:
    normalized = dict(creds)
    private_key = normalized.get("private_key")
    if isinstance(private_key, str):
        normalized["private_key"] = private_key.replace("\\n", "\n")
    return normalized


def load_google_creds() -> dict:
    if "gcp_service_account" in st.secrets:
        return _normalize_creds(dict(st.secrets["gcp_service_account"]))

    if "GOOGLE_CREDS" in st.secrets:
        raw = st.secrets["GOOGLE_CREDS"]
        parsed = raw if isinstance(raw, dict) else json.loads(raw)
        return _normalize_creds(parsed)

    raw_env = os.getenv("GOOGLE_CREDS")
    if not raw_env:
        raise RuntimeError(
            "Missing Google credentials. Set [gcp_service_account] in Streamlit secrets "
            "or GOOGLE_CREDS in environment."
        )

    return _normalize_creds(json.loads(raw_env))


@st.cache_resource
def get_bigquery_client() -> bigquery.Client:
    creds = load_google_creds()
    return bigquery.Client.from_service_account_info(creds)


def query_to_dataframe(query: str) -> pd.DataFrame:
    client = get_bigquery_client()
    return client.query(query).to_dataframe()
