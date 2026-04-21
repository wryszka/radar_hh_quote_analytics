# Databricks notebook source
# Config for Radar HH Quote Analytics demo.
# Change CATALOG here to re-point the demo to another workspace.

CATALOG = "lr_serverless_aws_us_catalog"
SCHEMA = "radar_hh_quote_analytics"

TABLE_SF_REQUESTS = "1_raw_salesforce_requests"
TABLE_RADAR_REQUESTS = "1_raw_radar_requests"
TABLE_RADAR_RESPONSES = "1_raw_radar_responses"
TABLE_QUOTES_FLAT = "2_silver_quotes_flat"

VOLUME = "saved_payloads"  # UC volume for "save as JSON" button
