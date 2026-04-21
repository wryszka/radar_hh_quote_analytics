"""SQL client wrapper using the Databricks SDK statement execution API.

Works both on Databricks Apps (SP auth via env) and on a laptop (profile).
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")


@st.cache_resource
def _client() -> WorkspaceClient:
    return WorkspaceClient()


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Supports :name parameter binding."""
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID not set — app.yaml binds it in Databricks Apps."
        )

    # Inline named parameters safely. Statement Execution API supports
    # parameters=[{name, value}] but keeping this simple for readability.
    if params:
        for k, v in params.items():
            if isinstance(v, str):
                sql = sql.replace(f":{k}", "'" + v.replace("'", "''") + "'")
            elif v is None:
                sql = sql.replace(f":{k}", "NULL")
            else:
                sql = sql.replace(f":{k}", str(v))

    w = _client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error
        msg = err.message if err and err.message else "Query failed or timed out"
        raise RuntimeError(msg)

    schema_cols = resp.manifest.schema.columns
    cols = [c.name for c in schema_cols]
    rows = resp.result.data_array if resp.result and resp.result.data_array else []
    df = pd.DataFrame(rows, columns=cols)

    # Statement Execution API returns all values as strings — coerce based on
    # the reported column type so downstream code can treat numbers as numbers.
    _NUM_TYPES = {"INT", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "DECIMAL"}
    _BOOL_TYPES = {"BOOLEAN"}
    for c in schema_cols:
        t = (c.type_name.value if hasattr(c.type_name, "value") else str(c.type_name)).upper()
        if t in _NUM_TYPES:
            df[c.name] = pd.to_numeric(df[c.name], errors="coerce")
        elif t in _BOOL_TYPES:
            df[c.name] = df[c.name].map(lambda v: str(v).lower() == "true" if v is not None else None)
    return df
