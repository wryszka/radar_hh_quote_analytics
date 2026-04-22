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
import genie


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
    ["Transaction lookup", "Analytics", "Ask Genie"]
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
                "Buildings SI": f"£{int(m['buildings_si']):,}",
                "Contents SI": f"£{int(m['contents_si']):,}",
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


# ---------------------------------------------------------------------------
# Tab 2 — Analytics: outliers, drop-outs, price distributions
# ---------------------------------------------------------------------------
with tab_analytics:
    st.subheader("Quote stream at a glance")

    try:
        # Headline metrics
        stats = query(
            f"""
            SELECT
              COUNT(*) AS total_transactions,
              COUNT_IF(quote_status = 'BOUND') AS bound,
              COUNT_IF(quote_status = 'QUOTED') AS quoted_not_bound,
              COUNT_IF(quote_status = 'ABANDONED') AS abandoned,
              COUNT_IF(is_outlier) AS outliers,
              ROUND(AVG(CASE WHEN quote_status <> 'ABANDONED' THEN gross_premium END), 2) AS avg_premium,
              ROUND(PERCENTILE(gross_premium, 0.95), 2) AS p95_premium
            FROM {full_table(TABLE_QUOTES_FLAT)}
            """
        ).iloc[0]

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Transactions", f"{int(stats['total_transactions']):,}")
        m2.metric("Bound", f"{int(stats['bound']):,}",
                  f"{stats['bound']/stats['total_transactions']:.0%} conversion")
        m3.metric("Abandoned", f"{int(stats['abandoned']):,}",
                  f"{stats['abandoned']/stats['total_transactions']:.0%} drop-out",
                  delta_color="inverse")
        m4.metric("Avg premium", f"£{stats['avg_premium']:,.0f}")
        m5.metric("Outliers flagged", f"{int(stats['outliers']):,}", delta_color="inverse")
    except Exception as exc:
        st.error(f"Couldn't load summary: {exc}")
        st.stop()

    st.divider()

    col_out, col_funnel = st.columns(2)

    # --- Outliers: top-N by price, plus peer-group z-score ---
    with col_out:
        st.markdown("#### Outliers by gross premium")
        st.caption(
            "Hard threshold: any quote priced above p99 × 3 of its peer group "
            "(same postcode region × property type). These are the quotes worth "
            "replaying against Radar Base."
        )
        outliers = query(
            f"""
            WITH peers AS (
              SELECT region, property_type,
                     PERCENTILE(gross_premium, 0.99) AS p99
              FROM {full_table(TABLE_QUOTES_FLAT)}
              WHERE quote_status <> 'ABANDONED'
              GROUP BY region, property_type
            )
            SELECT q.transaction_id, q.customer_name, q.region, q.property_type,
                   ROUND(q.gross_premium, 2) AS gross_premium,
                   ROUND(p.p99, 2) AS peer_p99,
                   ROUND(q.gross_premium / p.p99, 2) AS vs_peer_p99,
                   q.model_version, q.is_outlier
            FROM {full_table(TABLE_QUOTES_FLAT)} q
            JOIN peers p USING (region, property_type)
            WHERE q.gross_premium > p.p99 * 3
            ORDER BY q.gross_premium DESC
            LIMIT 20
            """
        )
        if outliers.empty:
            st.success("No outliers beyond p99 × 3. Stream looks clean.")
        else:
            st.dataframe(outliers, use_container_width=True, hide_index=True)

    # --- Drop-out funnel ---
    with col_funnel:
        st.markdown("#### Quote journey funnel")
        st.caption("Where do customers drop out? SF request → Radar response → bound policy.")
        funnel = query(
            f"""
            SELECT channel,
                   COUNT(*) AS started,
                   COUNT_IF(quote_status <> 'ABANDONED') AS got_price,
                   COUNT_IF(quote_status = 'BOUND') AS bound
            FROM {full_table(TABLE_QUOTES_FLAT)}
            GROUP BY channel
            ORDER BY started DESC
            """
        )
        funnel["dropout_rate"] = (1 - funnel["got_price"] / funnel["started"]).round(3)
        funnel["bind_rate"] = (funnel["bound"] / funnel["started"]).round(3)
        st.dataframe(
            funnel.rename(columns={
                "channel": "Channel",
                "started": "Started",
                "got_price": "Priced",
                "bound": "Bound",
                "dropout_rate": "Drop-out %",
                "bind_rate": "Bind %",
            }).style.format({"Drop-out %": "{:.1%}", "Bind %": "{:.1%}"}),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # --- Premium distribution by region (excludes outliers to keep axis sensible) ---
    st.markdown("#### Premium distribution by region")
    st.caption("Excludes the flagged outliers so the shape of the honest book is visible.")
    dist = query(
        f"""
        SELECT region, gross_premium
        FROM {full_table(TABLE_QUOTES_FLAT)}
        WHERE quote_status <> 'ABANDONED'
          AND is_outlier = false
          AND gross_premium < 10000
        """
    )
    if not dist.empty:
        import plotly.express as px
        fig = px.box(dist, x="region", y="gross_premium", points=False,
                     labels={"gross_premium": "Gross premium (£)", "region": "Region"})
        fig.update_layout(height=380, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 3 — Genie agent
# ---------------------------------------------------------------------------
with tab_agent:
    st.subheader("Ask questions about the quote stream")
    st.caption(
        "Natural-language Q&A powered by a Databricks Genie space over "
        f"`{CATALOG}.{SCHEMA}.{TABLE_QUOTES_FLAT}`."
    )

    if not genie.is_configured():
        st.warning(
            "Genie space not wired up yet.\n\n"
            "**One-time setup:**\n"
            "1. Open Genie in the workspace and create a new space over the "
            f"`{CATALOG}.{SCHEMA}.{TABLE_QUOTES_FLAT}` table.\n"
            "2. Give it a short description (e.g. *Household insurance quote "
            "stream — transactions, prices, drop-outs, outliers*).\n"
            "3. Copy the space ID from the URL and set `GENIE_SPACE_ID` in "
            "`app/app.yaml`, then redeploy."
        )
        st.stop()

    # Conversation state
    if "genie_history" not in st.session_state:
        st.session_state.genie_history = []  # list of {role, content, sql?, df?}
    if "genie_conv_id" not in st.session_state:
        st.session_state.genie_conv_id = None

    # Suggested prompts
    suggestions = [
        "How many quotes were abandoned last week by channel?",
        "What's the average gross premium by region for bound policies?",
        "Show me the 10 most expensive quotes and the model version that priced them.",
        "Which property type has the highest drop-out rate?",
    ]
    cols = st.columns(len(suggestions))
    picked = None
    for i, s in enumerate(suggestions):
        if cols[i].button(s, key=f"sugg_{i}", use_container_width=True):
            picked = s

    # Render history
    for turn in st.session_state.genie_history:
        with st.chat_message(turn["role"]):
            st.markdown(turn["content"])
            if turn.get("sql"):
                with st.expander("SQL generated by Genie"):
                    st.code(turn["sql"], language="sql")
            if turn.get("df") is not None and not turn["df"].empty:
                st.dataframe(turn["df"], use_container_width=True, hide_index=True)

    user_msg = st.chat_input("Ask Genie about the quote stream...") or picked
    if user_msg:
        st.session_state.genie_history.append({"role": "user", "content": user_msg})
        with st.chat_message("user"):
            st.markdown(user_msg)

        with st.chat_message("assistant"):
            with st.spinner("Genie is thinking..."):
                try:
                    if st.session_state.genie_conv_id is None:
                        conv_id, msg_id = genie.start_conversation(user_msg)
                        st.session_state.genie_conv_id = conv_id
                    else:
                        msg_id = genie.follow_up(st.session_state.genie_conv_id, user_msg)
                    ans = genie.wait_for_answer(st.session_state.genie_conv_id, msg_id)
                except Exception as exc:
                    st.error(f"Genie call failed: {exc}")
                    st.stop()

            if ans.error:
                st.error(f"Genie returned an error: {ans.error}")
            else:
                reply_md = ans.text or "_(Genie returned a SQL result — see below.)_"
                st.markdown(reply_md)
                if ans.sql:
                    with st.expander("SQL generated by Genie"):
                        st.code(ans.sql, language="sql")
                if ans.result is not None and not ans.result.empty:
                    st.dataframe(ans.result, use_container_width=True, hide_index=True)

                st.session_state.genie_history.append({
                    "role": "assistant",
                    "content": reply_md,
                    "sql": ans.sql,
                    "df": ans.result,
                })

    if st.session_state.genie_history:
        if st.button("Reset conversation", type="secondary"):
            st.session_state.genie_history = []
            st.session_state.genie_conv_id = None
            st.rerun()
