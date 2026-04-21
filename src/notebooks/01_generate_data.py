# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic Radar HH quote data
# MAGIC
# MAGIC Simulates the three JSON payloads captured by the Salesforce ↔ Radar Live traffic interceptor:
# MAGIC
# MAGIC 1. **Salesforce request** — incoming quote request from Salesforce
# MAGIC 2. **Radar Live request** — outgoing call to WTW Radar Live
# MAGIC 3. **Radar Live response** — full pricing response with loadings
# MAGIC
# MAGIC Plus a flattened silver table keyed by `transaction_id` for analytics and Genie.
# MAGIC
# MAGIC Seeds ~1,000 quotes including deliberate outliers (the "£3M grandma") and drop-outs.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

random.seed(42)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference pools

# COMMAND ----------

FIRST_NAMES = ["James", "Olivia", "Mohammed", "Priya", "George", "Emma", "Liam", "Sophie",
               "Arjun", "Chloe", "Oliver", "Amelia", "Hassan", "Isla", "Noah", "Ava",
               "William", "Mia", "Jack", "Grace", "Harry", "Ella", "Thomas", "Lily",
               "Jane", "Margaret", "Dorothy", "Betty", "Edith", "Joan"]
LAST_NAMES = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies",
              "Patel", "Khan", "Wright", "Green", "Hall", "Wood", "Walker", "Hughes",
              "Edwards", "Clarke", "Wright", "Roberts", "Thompson", "Evans"]
PROPERTY_TYPES = ["Detached", "Semi-Detached", "Terraced", "End-Terrace", "Flat", "Bungalow"]
ROOF_TYPES = ["Tiled", "Slated", "Flat", "Thatched"]
WALL_TYPES = ["Brick", "Stone", "Timber", "Rendered"]
POSTCODE_AREAS = [
    ("SW1A", "London", 1.35),     # (area, region, loading multiplier)
    ("E14",  "London", 1.40),
    ("M1",   "Manchester", 1.10),
    ("B1",   "Birmingham", 1.15),
    ("LS1",  "Leeds", 1.00),
    ("NE1",  "Newcastle", 0.95),
    ("BS1",  "Bristol", 1.05),
    ("CT1",  "Chatham", 0.90),
    ("YO1",  "York", 0.85),
    ("EX1",  "Exeter", 0.92),
]
FLOOD_ZONES = ["Low", "Medium", "High"]  # weight: low most common
PREVIOUS_INSURERS = ["Direct Line", "Aviva", "LV=", "Admiral", "Hastings", "Churchill", "None"]
CHANNELS = ["Direct", "Aggregator", "Broker", "Renewal"]
SF_USERS = ["sf.agent.01", "sf.agent.02", "sf.agent.03", "sf.self_service", "sf.broker.portal"]
MODEL_VERSIONS = ["HH_2025_Q4_v2", "HH_2026_Q1_v1", "HH_2026_Q1_v2", "HH_2026_Q2_v1"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payload builders

# COMMAND ----------

def random_postcode(area: str) -> str:
    return f"{area} {random.randint(1,9)}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"

def pricing_factors(property_type: str, year_built: int, bedrooms: int,
                    buildings_si: int, contents_si: int, flood_zone: str,
                    claims_5y: int, postcode_loading: float) -> dict:
    base_building = buildings_si * 0.0009
    base_contents = contents_si * 0.012
    base = (base_building + base_contents) * postcode_loading

    loadings = {}
    if flood_zone == "High":
        loadings["flood_risk"] = round(base * 0.35, 2)
    elif flood_zone == "Medium":
        loadings["flood_risk"] = round(base * 0.12, 2)

    if year_built < 1920:
        loadings["subsidence_age"] = round(base * 0.08, 2)

    if property_type in ("Flat", "Terraced", "End-Terrace"):
        loadings["escape_of_water"] = round(base * 0.04, 2)

    if claims_5y > 0:
        loadings["claims_history"] = round(base * 0.15 * claims_5y, 2)

    discounts = {}
    if claims_5y == 0:
        discounts["no_claims"] = round(base * 0.10, 2)
    if random.random() < 0.3:
        discounts["multi_product"] = round(base * 0.08, 2)

    return {
        "base_building_premium": round(base_building, 2),
        "base_contents_premium": round(base_contents, 2),
        "postcode_loading_factor": postcode_loading,
        "base_premium": round(base, 2),
        "loadings": loadings,
        "discounts": discounts,
    }

def build_quote(transaction_id: str, created_at: datetime, force_outlier: bool = False,
                force_dropout: bool = False) -> dict:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    area, region, pc_loading = random.choice(POSTCODE_AREAS)
    postcode = random_postcode(area)
    prop_type = random.choice(PROPERTY_TYPES)
    year_built = random.randint(1880, 2023)
    bedrooms = random.randint(1, 6)
    flood_zone = random.choices(FLOOD_ZONES, weights=[0.75, 0.20, 0.05])[0]
    claims_5y = random.choices([0, 1, 2, 3], weights=[0.72, 0.20, 0.06, 0.02])[0]
    buildings_si = random.choice([250_000, 350_000, 450_000, 600_000, 800_000, 1_200_000])
    contents_si = random.choice([25_000, 50_000, 75_000, 100_000, 150_000])
    channel = random.choice(CHANNELS)

    # ---- Salesforce request ----
    sf_request = {
        "salesforce_transaction_id": transaction_id,
        "timestamp": created_at.isoformat(),
        "agent_user": random.choice(SF_USERS),
        "channel": channel,
        "customer": {
            "first_name": first,
            "last_name": last,
            "dob": (created_at - timedelta(days=random.randint(25, 80) * 365)).date().isoformat(),
            "email": f"{first.lower()}.{last.lower()}@example.com",
        },
        "property": {
            "address_line_1": f"{random.randint(1, 200)} {random.choice(['High', 'Church', 'Mill', 'Park', 'Station'])} {random.choice(['Street', 'Road', 'Lane', 'Avenue'])}",
            "postcode": postcode,
            "region": region,
            "property_type": prop_type,
            "year_built": year_built,
            "bedrooms": bedrooms,
            "bathrooms": random.randint(1, 3),
            "roof_type": random.choice(ROOF_TYPES),
            "wall_type": random.choice(WALL_TYPES),
            "flood_zone": flood_zone,
        },
        "occupancy": {
            "owner_occupied": random.random() > 0.1,
            "unoccupied_days_per_year": random.choice([0, 0, 0, 7, 14, 30, 60]),
            "business_use": random.random() < 0.05,
        },
        "history": {
            "claims_last_5_years": claims_5y,
            "previous_insurer": random.choice(PREVIOUS_INSURERS),
            "ncd_years": random.choice([0, 1, 2, 3, 4, 5, 5, 5]),
        },
        "coverage_requested": {
            "buildings_sum_insured": buildings_si,
            "contents_sum_insured": contents_si,
            "voluntary_excess": random.choice([100, 250, 500]),
            "accidental_damage": random.random() < 0.4,
            "legal_expenses": random.random() < 0.6,
        },
    }

    # ---- Radar Live request (model input schema) ----
    radar_request = {
        "quote_reference": f"RL-{uuid.uuid4().hex[:10].upper()}",
        "salesforce_transaction_id": transaction_id,
        "model_version": random.choice(MODEL_VERSIONS),
        "timestamp": created_at.isoformat(),
        "factors": {
            "RATING_FACTOR_POSTCODE_AREA": area,
            "RATING_FACTOR_REGION": region,
            "RATING_FACTOR_PROPERTY_TYPE": prop_type,
            "RATING_FACTOR_PROPERTY_AGE": datetime.now().year - year_built,
            "RATING_FACTOR_BEDROOMS": bedrooms,
            "RATING_FACTOR_ROOF_TYPE": sf_request["property"]["roof_type"],
            "RATING_FACTOR_WALL_TYPE": sf_request["property"]["wall_type"],
            "RATING_FACTOR_FLOOD_ZONE": flood_zone,
            "RATING_FACTOR_CLAIMS_5Y": claims_5y,
            "RATING_FACTOR_NCD_YEARS": sf_request["history"]["ncd_years"],
            "RATING_FACTOR_BUILDINGS_SI": buildings_si,
            "RATING_FACTOR_CONTENTS_SI": contents_si,
            "RATING_FACTOR_VOL_EXCESS": sf_request["coverage_requested"]["voluntary_excess"],
            "RATING_FACTOR_BUSINESS_USE": sf_request["occupancy"]["business_use"],
            "RATING_FACTOR_UNOCCUPIED_DAYS": sf_request["occupancy"]["unoccupied_days_per_year"],
            "RATING_FACTOR_CHANNEL": channel,
        },
    }

    # ---- Radar Live response ----
    if force_dropout:
        return sf_request, radar_request, None, None  # no response, abandoned

    factors = pricing_factors(prop_type, year_built, bedrooms, buildings_si, contents_si,
                              flood_zone, claims_5y, pc_loading)

    net_premium = factors["base_premium"] + sum(factors["loadings"].values()) - sum(factors["discounts"].values())

    if force_outlier:
        # The infamous £3M quote — simulate a bad factor explosion
        net_premium = 3_014_827.55
        factors["loadings"]["model_anomaly"] = 3_000_000.0

    ipt = round(net_premium * 0.12, 2)  # UK Insurance Premium Tax 12%
    gross_premium = round(net_premium + ipt, 2)

    radar_response = {
        "quote_reference": radar_request["quote_reference"],
        "salesforce_transaction_id": transaction_id,
        "model_version": radar_request["model_version"],
        "timestamp": (created_at + timedelta(milliseconds=random.randint(120, 890))).isoformat(),
        "pricing": {
            "base_building_premium": factors["base_building_premium"],
            "base_contents_premium": factors["base_contents_premium"],
            "postcode_loading_factor": factors["postcode_loading_factor"],
            "base_premium": factors["base_premium"],
            "loadings": factors["loadings"],
            "discounts": factors["discounts"],
            "net_premium": round(net_premium, 2),
            "ipt": ipt,
            "gross_premium": gross_premium,
        },
        "decision": {
            "status": "QUOTED",
            "decline_reason": None,
            "quote_expiry": (created_at + timedelta(days=30)).date().isoformat(),
        },
    }

    return sf_request, radar_request, radar_response, gross_premium

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate rows

# COMMAND ----------

N_QUOTES = 1000
DROPOUT_RATE = 0.18   # ~18% abandon the journey
OUTLIER_TRANSACTION_IDS = ["TX-C051-3M-GRANDMA", "TX-A914-OUTLIER-02"]

sf_rows, radar_req_rows, radar_resp_rows, silver_rows = [], [], [], []

base_time = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)

for i in range(N_QUOTES):
    created_at = base_time + timedelta(minutes=random.randint(0, 30 * 24 * 60))

    if i < len(OUTLIER_TRANSACTION_IDS):
        tx_id = OUTLIER_TRANSACTION_IDS[i]
        force_outlier, force_dropout = True, False
    else:
        tx_id = f"TX-{uuid.uuid4().hex[:8].upper()}"
        force_dropout = random.random() < DROPOUT_RATE
        force_outlier = False

    sf_req, rl_req, rl_resp, gross = build_quote(tx_id, created_at,
                                                  force_outlier=force_outlier,
                                                  force_dropout=force_dropout)

    sf_rows.append(Row(transaction_id=tx_id, created_at=created_at,
                       payload=json.dumps(sf_req)))
    radar_req_rows.append(Row(transaction_id=tx_id, created_at=created_at,
                              payload=json.dumps(rl_req)))

    bound = False
    if rl_resp is not None:
        radar_resp_rows.append(Row(transaction_id=tx_id,
                                   created_at=datetime.fromisoformat(rl_resp["timestamp"]),
                                   payload=json.dumps(rl_resp)))
        # ~60% of quoted convert to bound policies
        bound = random.random() < 0.60

    status = "ABANDONED" if rl_resp is None else ("BOUND" if bound else "QUOTED")

    silver_rows.append(Row(
        transaction_id=tx_id,
        created_at=created_at,
        channel=sf_req["channel"],
        agent_user=sf_req["agent_user"],
        customer_name=f'{sf_req["customer"]["first_name"]} {sf_req["customer"]["last_name"]}',
        postcode=sf_req["property"]["postcode"],
        region=sf_req["property"]["region"],
        property_type=sf_req["property"]["property_type"],
        year_built=sf_req["property"]["year_built"],
        bedrooms=sf_req["property"]["bedrooms"],
        flood_zone=sf_req["property"]["flood_zone"],
        claims_last_5y=sf_req["history"]["claims_last_5_years"],
        buildings_si=sf_req["coverage_requested"]["buildings_sum_insured"],
        contents_si=sf_req["coverage_requested"]["contents_sum_insured"],
        model_version=rl_req["model_version"],
        gross_premium=float(gross) if gross is not None else None,
        quote_status=status,
        is_outlier=force_outlier,
    ))

print(f"Generated {len(sf_rows)} SF requests, {len(radar_resp_rows)} Radar responses, "
      f"{len(silver_rows)} silver rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write tables

# COMMAND ----------

def write_raw(rows, table_name):
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("payload", StringType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"))
    print(f"Wrote {df.count()} rows to {CATALOG}.{SCHEMA}.{table_name}")

write_raw(sf_rows, TABLE_SF_REQUESTS)
write_raw(radar_req_rows, TABLE_RADAR_REQUESTS)
write_raw(radar_resp_rows, TABLE_RADAR_RESPONSES)

# COMMAND ----------

silver_df = spark.createDataFrame(silver_rows)
(silver_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{TABLE_QUOTES_FLAT}`"))
print(f"Wrote {silver_df.count()} rows to {CATALOG}.{SCHEMA}.{TABLE_QUOTES_FLAT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT quote_status, COUNT(*) AS n,
           ROUND(AVG(gross_premium), 2) AS avg_gross,
           ROUND(MAX(gross_premium), 2) AS max_gross
    FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE_QUOTES_FLAT}`
    GROUP BY quote_status
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT transaction_id, customer_name, postcode, gross_premium, is_outlier
    FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE_QUOTES_FLAT}`
    WHERE is_outlier = true
"""))
