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

    # Statement Execution API returns all values as strings. Coerce types:
    # - Columns flagged BOOLEAN by the schema → native bool
    # - Every other column that parses cleanly as a number → numeric
    # Using errors='ignore' keeps genuine string columns intact.
    _BOOL_NAMES = {"BOOLEAN", "BOOL"}
    for c in schema_cols:
        raw = c.type_name
        t_name = getattr(raw, "name", None) or getattr(raw, "value", None) or str(raw)
        t_name = t_name.split(".")[-1].upper()
        if t_name in _BOOL_NAMES:
            df[c.name] = df[c.name].map(
                lambda v: None if v is None else str(v).lower() == "true"
            )
            continue
        try:
            df[c.name] = pd.to_numeric(df[c.name], errors="raise")
        except (ValueError, TypeError):
            pass  # leave as string
    return df
