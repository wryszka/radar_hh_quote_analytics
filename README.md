# Radar HH Quote Analytics

Streamlit app on Databricks Apps for investigating household (HH) insurance quotes flowing through Salesforce → API → WTW Radar Live. Captures the three JSON payloads per transaction (Salesforce request, Radar Live request, Radar Live response) and provides lookup, flattening, replay, analytics, and natural-language Q&A over the stream.

## About this demo

This is a demonstration asset for Field Engineering. Synthetic data only — no real customer or policy information is used. The Radar Live integration is simulated; in a production deployment the app would be pointed at a real Unity Catalog stream fed by the Salesforce ↔ Radar Live traffic capture.

## Build order (simple → complex)

| Step | Scope | Status |
| --- | --- | --- |
| 1 | Scaffold + synthetic data + MVP lookup app | in progress |
| 2 | Analytics tab (outliers, drop-outs, price thresholds) | pending |
| 3 | Genie agent tab for natural-language Q&A | pending |
| 4 | Deploy to Databricks Apps | pending |

## Data model

Single schema `radar_hh_quote_analytics` with numbered tables:

- `1_raw_salesforce_requests` — incoming API request from Salesforce (JSON)
- `1_raw_radar_requests` — outgoing request to Radar Live (JSON)
- `1_raw_radar_responses` — full response from Radar Live with loadings applied (JSON)
- `2_silver_quotes_flat` — one row per transaction, flattened fields for analytics

All keyed by `transaction_id`.

## Workspace

- Host: `https://fevm-lr-serverless-aws-us.cloud.databricks.com`
- Profile: `DEFAULT`
- Catalog: `lr_serverless_aws_us_catalog`
- Schema: `radar_hh_quote_analytics`

Catalog is portable — change via:
```bash
sed -i '' 's/lr_serverless_aws_us_catalog/<your_catalog>/g' src/notebooks/*.py app/config.py
```

## Local dev

```bash
uv venv --native-tls
source .venv/bin/activate
uv pip install --native-tls -r app/requirements.txt
streamlit run app/app.py
```
