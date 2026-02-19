# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, coalesce, current_timestamp, round
from datetime import datetime

print("Libraries imported")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

CATALOG = "workspace"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Silver tables
SILVER_CIRCUITS = f"{CATALOG}.{SILVER_SCHEMA}.silver_circuits"
SILVER_INSTALLED_DER = f"{CATALOG}.{SILVER_SCHEMA}.silver_installed_der"
SILVER_PLANNED_DER = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der"

# Gold table
GOLD_FEEDER_SUMMARY = f"{CATALOG}.{GOLD_SCHEMA}.gold_circuit_summary"


print("Table Configuration:")
print(f"   Silver Circuits:      {SILVER_CIRCUITS}")
print(f"   Silver Installed DER: {SILVER_INSTALLED_DER}")
print(f"   Silver Planned DER:   {SILVER_PLANNED_DER}")
print(f"   Gold Target:          {GOLD_FEEDER_SUMMARY}")




# COMMAND ----------

# Load active circuits only
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
print("Aggregating DER data by feeder...")
print("="*80)

# Aggregate installed DER by feeder
df_installed_agg = df_installed.groupBy("project_circuit_id", "utility_id").agg(
    count("install_der_id").alias("installed_count"),
    sum("DER_VALUE").alias("installed_capacity_mw")
)

print(f"Installed DER aggregated: {df_installed_agg.count():,} feeders")

# Aggregate planned DER by feeder
df_planned_agg = df_planned.groupBy("project_circuit_id", "utility_id").agg(
    count("project_id_pk").alias("planned_count"),
    sum("DER_VALUE").alias("planned_capacity_mw")
)

print(f"Planned DER aggregated: {df_planned_agg.count():,} feeders")


print("\n" + "="*80)
print("Joining circuits with DER aggregations...")
print("="*80)

# Join circuits with installed DER (LEFT JOIN - circuits without DER still show)
df_gold = df_circuits.select(
    col("circuit"),
    col("utility_id"),
    col("fmaxhc").alias("max_capacity_mw"),
    col("fvoltage").alias("voltage_kv"),
    col("refresh_date").alias("last_refresh_date")
)

# Join with installed DER aggregation
df_gold = df_gold.join(
    df_installed_agg,
    (df_gold["circuit"] == df_installed_agg["project_circuit_id"]) &
    (df_gold["utility_id"] == df_installed_agg["utility_id"]),
    "left"
).select(
    df_gold["*"],
    coalesce(df_installed_agg["installed_count"], F.lit(0)).alias("installed_count"),
    coalesce(df_installed_agg["installed_capacity_mw"], F.lit(0.0)).alias("installed_capacity_mw")
)

# Join with planned DER aggregation
df_gold = df_gold.join(
    df_planned_agg,
    (df_gold["circuit"] == df_planned_agg["project_circuit_id"]) &
    (df_gold["utility_id"] == df_planned_agg["utility_id"]),
    "left"
).select(
    df_gold["*"],
    coalesce(df_planned_agg["planned_count"], F.lit(0)).alias("planned_count"),
    coalesce(df_planned_agg["planned_capacity_mw"], F.lit(0.0)).alias("planned_capacity_mw")
)

print(f"Joined: {df_gold.count():,} feeders")

df_gold = df_gold.withColumn(
    "available_capacity_mw",
    round(col("max_capacity_mw") - col("installed_capacity_mw"), 4)
).withColumn(
    "gold_load_timestamp",
    current_timestamp()
)

print("Calculated metrics")




# COMMAND ----------

print("\n" + "="*80)
print("Loading to Gold table...")
print("="*80)

record_count = df_gold.count()
print(f"Records to load: {record_count:,}")


# Overwrite Gold table (full refresh)
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_FEEDER_SUMMARY)

print(f"Loaded {record_count:,} records to {GOLD_FEEDER_SUMMARY}")
