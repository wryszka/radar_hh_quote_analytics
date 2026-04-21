"""Shared config for the Streamlit app. Keep in sync with src/notebooks/00_config.py."""
import os

CATALOG = os.getenv("DEMO_CATALOG", "lr_serverless_aws_us_catalog")
SCHEMA = os.getenv("DEMO_SCHEMA", "radar_hh_quote_analytics")

TABLE_SF_REQUESTS = "1_raw_salesforce_requests"
TABLE_RADAR_REQUESTS = "1_raw_radar_requests"
TABLE_RADAR_RESPONSES = "1_raw_radar_responses"
TABLE_QUOTES_FLAT = "2_silver_quotes_flat"

VOLUME = "saved_payloads"

# Genie space for the agent tab — set via env / app.yaml after creation.
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")

# SQL warehouse — set via env / app.yaml resource binding.
SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")


def full_table(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def volume_path() -> str:
    return f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
