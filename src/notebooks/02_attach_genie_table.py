# Databricks notebook source
# MAGIC %md
# MAGIC # Attach silver quotes table to the Radar HH Genie space
# MAGIC
# MAGIC The public REST PATCH on `/api/2.0/data-rooms/{id}` silently drops the
# MAGIC `serialized_space` payload. The SDK's `w.genie.update_space()` does not.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

SPACE_ID = "01f13e0c456d1ddba8bf1f32514d0db4"
TABLE_FQN = "lr_serverless_aws_us_catalog.radar_hh_quote_analytics.2_silver_quotes_flat"

GENIE_INSTRUCTIONS = """
You are an assistant for pricing actuaries and operations staff at a UK household
insurer. You answer questions about the live Household quote stream that flows
Salesforce → API → WTW Radar Live → API → Salesforce. Each row of
`2_silver_quotes_flat` is one transaction captured from that traffic.

## Data model (single table)
- `transaction_id`: primary key, ties back to Salesforce
- `created_at`: UTC timestamp of the Salesforce request
- `channel`: Direct, Aggregator, Broker, Renewal
- `agent_user`: Salesforce agent / self-service / broker portal identifier
- `customer_name`, `postcode`, `region`, `property_type`, `year_built`,
  `bedrooms`, `flood_zone`, `claims_last_5y`: risk attributes
- `buildings_si`, `contents_si`: sums insured (£)
- `model_version`: Radar Live model version used (e.g. `HH_2026_Q2_v1`)
- `gross_premium`: final quote (£, includes 12% IPT). NULL when abandoned.
- `quote_status`: `BOUND` (customer bought), `QUOTED` (priced but not bound),
  `ABANDONED` (no Radar response recorded — customer dropped out)
- `is_outlier`: boolean, true for deliberately seeded anomalies

## Key concepts
- Drop-out = `quote_status = 'ABANDONED'`. Drop-out rate = abandoned / total.
- Bind rate = BOUND / total.
- Peer group = (region, property_type). Outliers are worth replaying against
  Radar Base.
- All prices in GBP; format with £ and thousand separators.
- "Channel" decomposes the journey; "region" decomposes the book.

## Common questions
- Drop-out rate by channel → `COUNT_IF(quote_status='ABANDONED') / COUNT(*)`
  grouped by channel.
- Top outliers → WHERE is_outlier OR gross_premium > 3 × peer-group p99.
- Average premium by region (bound policies only) → exclude ABANDONED.
- Model-version mix over time → GROUP BY model_version, DATE_TRUNC('day', created_at).
""".strip()

SAMPLE_QUESTIONS = [
    "How many quotes were abandoned by channel, and what's the drop-out rate?",
    "What's the average gross premium by region for bound policies?",
    "Show the 10 most expensive quotes and the model version that priced them.",
    "Which property type has the highest drop-out rate?",
    "Which postcodes in flood zone High have the biggest premium vs peer-group average?",
    "What's the bind rate by channel for the last 7 days?",
]

# COMMAND ----------

w = WorkspaceClient()

# Check current state
current = w.genie.get_space(space_id=SPACE_ID)
print(f"Before: title={current.title}")

# COMMAND ----------

# Attach the table via serialized_space
payload = {
    "version": 2,
    "data_sources": {
        "tables": [{"identifier": TABLE_FQN}],
    },
}

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(payload),
)
print(f"Attached table: {TABLE_FQN}")

# COMMAND ----------

# Verify
after = w.api_client.do(
    "GET",
    f"/api/2.0/data-rooms/{SPACE_ID}?include_serialized_space=true",
)
print("After serialized_space:")
print(after.get("serialized_space"))

# COMMAND ----------

# Smoke-test the space with a real Genie question
import time
start = w.api_client.do(
    "POST",
    f"/api/2.0/genie/spaces/{SPACE_ID}/start-conversation",
    body={"content": "How many quotes are in the data, and how many were abandoned?"},
)
conv_id = start["conversation_id"]
msg_id = start["message_id"]

for _ in range(30):
    msg = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conv_id}/messages/{msg_id}",
    )
    if msg["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
        break
    time.sleep(2)

for att in msg.get("attachments", []):
    if att.get("text"):
        print("TEXT:", att["text"]["content"])
    if att.get("query"):
        print("SQL:", att["query"]["query"])
