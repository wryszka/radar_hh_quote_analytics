"""Small Databricks SQL client wrapper for the Streamlit app.

Uses the Databricks SDK for workspace-resolved auth (works on Databricks Apps
out of the box via the app's service principal and the DATABRICKS_HOST /
DATABRICKS_WAREHOUSE_ID env vars wired by app.yaml). Falls back to a local
profile for dev on a laptop.
"""
from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
from databricks import sql as dbsql
from databricks.sdk.core import Config


@lru_cache(maxsize=1)
def _config() -> Config:
    # Databricks Apps sets DATABRICKS_HOST/DATABRICKS_CLIENT_ID etc. Local dev
    # uses ~/.databrickscfg profile DEFAULT.
    if os.getenv("DATABRICKS_CLIENT_ID"):
        return Config()  # auto: SP in Apps runtime
    return Config(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))


def _warehouse_path() -> str:
    wh_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("SQL_WAREHOUSE_ID")
    if not wh_id:
        raise RuntimeError(
            "SQL warehouse not configured. Set DATABRICKS_WAREHOUSE_ID env var "
            "(app.yaml binds it automatically in Databricks Apps)."
        )
    return f"/sql/1.0/warehouses/{wh_id}"


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    cfg = _config()
    with dbsql.connect(
        server_hostname=cfg.host.replace("https://", "").rstrip("/"),
        http_path=_warehouse_path(),
        credentials_provider=lambda: cfg.authenticate,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, parameters=params or {})
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)
