"""Radar HH Quote Analytics — Streamlit on Databricks Apps.

Step 1 (MVP): transaction ID lookup, JSON payload viewing, save-to-volume.
Step 2: analytics (outliers, drop-outs).
Step 3: Genie agent for natural-language Q&A.
"""
from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import streamlit as st

from config import (
    CATALOG,
    SCHEMA,
    TABLE_SF_REQUESTS,
    TABLE_RADAR_REQUESTS,
    TABLE_RADAR_RESPONSES,
    TABLE_QUOTES_FLAT,
    full_table,
    volume_path,
)
from db import query

st.set_page_config(page_title="Radar HH Quote Analytics", layout="wide", page_icon=":mag:")

st.title("Radar HH Quote Analytics")
st.caption(
    f"Household quote stream · Salesforce → API → Radar Live · "
    f"`{CATALOG}.{SCHEMA}`"
)

with st.expander("About this demo", expanded=False):
    st.markdown(
        """
This is a Field Engineering demonstration asset. All data is **synthetic** —
no real customer or policy information is used, and the Radar Live integration
is simulated.

The demo illustrates how Databricks can host the three JSON payloads captured
from the Salesforce ↔ Radar Live traffic (Salesforce request, Radar Live
request, Radar Live response) and provide lookup, replay, analytics, and
natural-language Q&A on top.
        """
    )

tab_lookup, tab_analytics, tab_agent = st.tabs(
    ["Transaction lookup", "Analytics (coming in step 2)", "Agent (coming in step 3)"]
)

# ---------------------------------------------------------------------------
# Tab 1 — Transaction lookup (MVP per Tom's verbal spec)
# ---------------------------------------------------------------------------
with tab_lookup:
    left, right = st.columns([1, 2])

    with left:
        st.subheader("Find a quote")
        st.markdown("Start from a **transaction ID** (e.g. `TX-C051-3M-GRANDMA`) "
                    "or pick one from the recent list below.")

        tx_input = st.text_input("Transaction ID", placeholder="TX-XXXXXXXX")

        st.markdown("**Recent quotes** (click to load):")
        try:
            recent = query(
                f"""
                SELECT transaction_id, customer_name, postcode,
                       gross_premium, quote_status, is_outlier
                FROM {full_table(TABLE_QUOTES_FLAT)}
                ORDER BY created_at DESC
                LIMIT 50
                """
            )
            # Highlight outliers
            def _fmt(row):
                marker = " ⚠️" if row["is_outlier"] else ""
                price = f"£{row['gross_premium']:,.0f}" if row["gross_premium"] else "—"
                return f"{row['transaction_id']} · {row['customer_name']} · {price}{marker}"

            options = ["—"] + recent.apply(_fmt, axis=1).tolist()
            picked = st.selectbox("", options, label_visibility="collapsed")
            if picked != "—":
                tx_input = picked.split(" · ")[0]
        except Exception as exc:
            st.error(f"Couldn't load recent quotes: {exc}")
            recent = pd.DataFrame()

    with right:
        if not tx_input:
            st.info("Enter a transaction ID on the left to view its payloads.")
            st.stop()

        st.subheader(f"Transaction `{tx_input}`")

        # Pull all 3 payloads + silver row in one go
        try:
            meta = query(
                f"""
                SELECT * FROM {full_table(TABLE_QUOTES_FLAT)}
                WHERE transaction_id = :tx
                """,
                {"tx": tx_input},
            )
            sf_df = query(
                f"SELECT payload FROM {full_table(TABLE_SF_REQUESTS)} WHERE transaction_id = :tx",
                {"tx": tx_input},
            )
            rl_req_df = query(
                f"SELECT payload FROM {full_table(TABLE_RADAR_REQUESTS)} WHERE transaction_id = :tx",
                {"tx": tx_input},
            )
            rl_resp_df = query(
                f"SELECT payload FROM {full_table(TABLE_RADAR_RESPONSES)} WHERE transaction_id = :tx",
                {"tx": tx_input},
            )
        except Exception as exc:
            st.error(f"Query failed: {exc}")
            st.stop()

        if meta.empty:
            st.warning(f"No quote found for `{tx_input}`.")
            st.stop()

        m = meta.iloc[0]

        # Summary cards
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Status", m["quote_status"])
        c2.metric("Gross premium",
                  f"£{m['gross_premium']:,.0f}" if m["gross_premium"] else "—")
        c3.metric("Postcode", m["postcode"])
        c4.metric("Model", m["model_version"])

        if m["is_outlier"]:
            st.error(
                "⚠️ **Outlier detected** — this quote sits far outside the peer-group "
                "distribution. Use the Radar Live response below to replay against "
                "Radar Base."
            )

        # Flattened customer / property view (what Tom asked for: a flattened view
        # of the JSON, not just raw nested blob)
        with st.expander("Flattened risk summary", expanded=True):
            flat = {
                "Customer": m["customer_name"],
                "Channel": m["channel"],
                "Agent user": m["agent_user"],
                "Region": m["region"],
                "Property type": m["property_type"],
                "Year built": int(m["year_built"]),
                "Bedrooms": int(m["bedrooms"]),
                "Flood zone": m["flood_zone"],
                "Claims last 5y": int(m["claims_last_5y"]),
                "Buildings SI": f"£{m['buildings_si']:,}",
                "Contents SI": f"£{m['contents_si']:,}",
            }
            st.dataframe(
                pd.DataFrame(flat.items(), columns=["Field", "Value"]),
                hide_index=True, use_container_width=True,
            )

        # Three payloads
        sub = st.tabs(["Salesforce request", "Radar Live request", "Radar Live response"])
        payloads = {
            "sf": json.loads(sf_df["payload"].iloc[0]) if not sf_df.empty else None,
            "rl_req": json.loads(rl_req_df["payload"].iloc[0]) if not rl_req_df.empty else None,
            "rl_resp": json.loads(rl_resp_df["payload"].iloc[0]) if not rl_resp_df.empty else None,
        }

        def _payload_panel(label: str, data: dict | None, key: str):
            if data is None:
                st.info(f"No {label} recorded — the journey was abandoned before this step.")
                return
            # Streamlit's st.code block includes a built-in copy button → that's
            # exactly what Tom asked for ("can you do a copy and paste function").
            pretty = json.dumps(data, indent=2)
            st.code(pretty, language="json", line_numbers=False)

            col_a, col_b = st.columns([1, 3])
            with col_a:
                st.download_button(
                    "Download JSON",
                    data=pretty,
                    file_name=f"{tx_input}_{key}.json",
                    mime="application/json",
                    key=f"dl_{key}",
                )
            with col_b:
                if st.button(f"Save to UC volume (`{volume_path()}`)", key=f"save_{key}"):
                    _save_to_volume(pretty, tx_input, key)

        with sub[0]:
            _payload_panel("Salesforce request", payloads["sf"], "sf")
        with sub[1]:
            _payload_panel("Radar Live request", payloads["rl_req"], "rl_req")
        with sub[2]:
            _payload_panel("Radar Live response", payloads["rl_resp"], "rl_resp")


def _save_to_volume(pretty_json: str, tx_id: str, key: str) -> None:
    """Save a payload JSON to a Unity Catalog volume using the SDK's Files API."""
    from databricks.sdk import WorkspaceClient

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"{volume_path()}/{tx_id}_{key}_{ts}.json"
    try:
        w = WorkspaceClient()
        w.files.upload(file_path=path, contents=pretty_json.encode("utf-8"), overwrite=True)
        st.success(f"Saved to `{path}`")
    except Exception as exc:
        st.error(f"Save failed: {exc}")


# ---------------------------------------------------------------------------
# Tab 2 — Analytics (step 2 placeholder)
# ---------------------------------------------------------------------------
with tab_analytics:
    st.info("**Coming in step 2** — peer-group outlier detection, price threshold "
            "alerts, drop-out funnel. Leaving this tab intentionally empty so we "
            "ship the MVP first.")

# ---------------------------------------------------------------------------
# Tab 3 — Genie agent (step 3 placeholder)
# ---------------------------------------------------------------------------
with tab_agent:
    st.info("**Coming in step 3** — ask natural-language questions over the quote "
            "stream via a Databricks Genie space.")
