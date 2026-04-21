"""Thin wrapper around the Databricks Genie Conversation API.

Docs: https://docs.databricks.com/api/workspace/genie
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


@dataclass
class GenieAnswer:
    text: str | None
    sql: str | None
    result: pd.DataFrame | None
    error: str | None = None


SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")


@st.cache_resource
def _w() -> WorkspaceClient:
    return WorkspaceClient()


def is_configured() -> bool:
    return bool(SPACE_ID)


def start_conversation(message: str) -> tuple[str, str]:
    """Start a new Genie conversation. Returns (conversation_id, message_id)."""
    w = _w()
    resp = w.api_client.do(
        "POST",
        f"/api/2.0/genie/spaces/{SPACE_ID}/start-conversation",
        body={"content": message},
    )
    return resp["conversation_id"], resp["message_id"]


def follow_up(conversation_id: str, message: str) -> str:
    """Send a follow-up message in an existing conversation. Returns message_id."""
    w = _w()
    resp = w.api_client.do(
        "POST",
        f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages",
        body={"content": message},
    )
    return resp["message_id"]


def wait_for_answer(conversation_id: str, message_id: str,
                    timeout_s: int = 120) -> GenieAnswer:
    """Poll until Genie finishes answering. Returns parsed answer + result df."""
    w = _w()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msg = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages/{message_id}",
        )
        status = msg.get("status")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            if status != "COMPLETED":
                return GenieAnswer(None, None, None,
                                   error=msg.get("error", {}).get("error", status))
            return _parse_answer(conversation_id, message_id, msg)
        time.sleep(1.5)
    return GenieAnswer(None, None, None, error=f"Timed out after {timeout_s}s")


def _parse_answer(conversation_id: str, message_id: str, msg: dict) -> GenieAnswer:
    text_parts: list[str] = []
    sql: str | None = None
    attachment_id: str | None = None

    for att in msg.get("attachments", []) or []:
        if att.get("text"):
            text_parts.append(att["text"].get("content", ""))
        if att.get("query"):
            sql = att["query"].get("query")
            attachment_id = att.get("attachment_id")

    df: pd.DataFrame | None = None
    if attachment_id:
        df = _fetch_query_result(conversation_id, message_id, attachment_id)

    return GenieAnswer(
        text="\n\n".join(t for t in text_parts if t) or None,
        sql=sql,
        result=df,
    )


def _fetch_query_result(conversation_id: str, message_id: str,
                        attachment_id: str) -> pd.DataFrame | None:
    """Genie executes its SQL itself — we just fetch the stored result."""
    w = _w()
    try:
        resp = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}"
            f"/messages/{message_id}/attachments/{attachment_id}/query-result",
        )
    except Exception:
        return None

    sr = resp.get("statement_response") or {}
    status = (sr.get("status") or {}).get("state")
    if status != StatementState.SUCCEEDED.value and status != "SUCCEEDED":
        return None
    cols = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = sr.get("result", {}).get("data_array") or []
    return pd.DataFrame(rows, columns=cols)
