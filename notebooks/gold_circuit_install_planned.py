# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

print("Libraries imported")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# Table configuration
CATALOG = "workspace"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Silver tables
SILVER_CIRCUITS = f"{CATALOG}.{SILVER_SCHEMA}.silver_circuits"
SILVER_INSTALLED_DER = f"{CATALOG}.{SILVER_SCHEMA}.silver_installed_der"
SILVER_PLANNED_DER = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der"

# Gold table
GOLD_FEEDER_DER_ALL = f"{CATALOG}.{GOLD_SCHEMA}.gold_circuit_install_planned"

print("Table Configuration:")
print(f"   Silver Circuits:      {SILVER_CIRCUITS}")
print(f"   Silver Installed DER: {SILVER_INSTALLED_DER}")
print(f"   Silver Planned DER:   {SILVER_PLANNED_DER}")
print(f"   Gold Target:          {GOLD_FEEDER_DER_ALL}")


# COMMAND ----------

df_circuits = spark.table(SILVER_CIRCUITS).filter(col("active") == True)
circuits_count = df_circuits.count()
print(f"Circuits (active): {circuits_count:,} records")

# Load active installed DER only
df_installed = spark.table(SILVER_INSTALLED_DER).filter(col("active") == True)
installed_count = df_installed.count()
print(f"Installed DER (active): {installed_count:,} records")

# Load active planned DER only
df_planned = spark.table(SILVER_PLANNED_DER).filter(col("active") == True)
planned_count = df_planned.count()
print(f"Planned DER (active): {planned_count:,} records")

print("\n" + "="*80)
print("Joining circuits with installed DER...")
print("="*80)

df_installed_gold = df_circuits.select(
    col("circuit"),
    col("utility_id")
).join(
    df_installed,
    (df_circuits["circuit"] == df_installed["project_circuit_id"]) &
    (df_circuits["utility_id"] == df_installed["utility_id"]),
    "inner"
).select(
    col("circuit"),
    df_circuits["utility_id"],
    lit("Installed").alias("der_category"),
    df_installed["project_id"],
    df_installed["DER_TYPE"].alias("der_type"),
    df_installed["DER_VALUE"].alias("der_value"),
    df_installed["nameplate_rating"],
    df_installed["street_address"],
    df_installed["der_interconn_loc"],
    lit(None).cast("string").alias("project_status"),
    lit(None).cast("date").alias("completion_date"),
    current_timestamp().alias("gold_load_timestamp")
)

installed_gold_count = df_installed_gold.count()
print(f"Installed DER joined: {installed_gold_count:,} records")


print("\n" + "="*80)
print("Joining circuits with planned DER...")
print("="*80)

df_planned_gold = df_circuits.select(
    col("circuit"),
    col("utility_id")
).join(
    df_planned,
    (df_circuits["circuit"] == df_planned["project_circuit_id"]) &
    (df_circuits["utility_id"] == df_planned["utility_id"]),
    "inner"
).select(
    col("circuit"),
    df_circuits["utility_id"],
    lit("Planned").alias("der_category"),
    df_planned["project_id"],
    df_planned["DER_TYPE"].alias("der_type"),
    df_planned["DER_VALUE"].alias("der_value"),
    df_planned["nameplate_rating"],
    lit(None).cast("string").alias("street_address"),  # Planned may not have street address
    df_planned["der_interconn_loc"],
    df_planned["project_status"],
    df_planned["completion_date"],
    current_timestamp().alias("gold_load_timestamp")
)

planned_gold_count = df_planned_gold.count()
print(f"Planned DER joined: {planned_gold_count:,} records")


print("\n" + "="*80)
print("Combining Installed + Planned DER...")
print("="*80)

# Union all installed and planned
df_gold_all = df_installed_gold.unionAll(df_planned_gold)

total_count = df_gold_all.count()
print(f"Total DER records: {total_count:,}")
print(f"   - Installed: {installed_gold_count:,}")
print(f"   - Planned:   {planned_gold_count:,}")




# COMMAND ----------

print("\n" + "="*80)
print("Loading to Gold table...")
print("="*80)

print(f"Records to load: {total_count:,}")

# Overwrite Gold table (full refresh)
df_gold_all.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_FEEDER_DER_ALL)

print(f"Loaded {total_count:,} records to {GOLD_FEEDER_DER_ALL}")
