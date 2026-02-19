# Databricks notebook source


# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F
from datetime import datetime

print("\n" + "="*80)
print("MERGING STAGING → SILVER (Type 2 SCD)")
print("="*80)

STAGING_TABLE="workspace.silver.silver_circuits_stg"
SILVER_TABLE="workspace.silver.silver_circuits"

# Check if staging has data
staging_count = spark.sql(f"SELECT COUNT(*) FROM {STAGING_TABLE}").collect()[0][0]
target_max_ts=spark.sql(f"SELECT MAX(ingestion_time) FROM {SILVER_TABLE}").collect()[0][0]


if staging_count == 0:
    print("No records in staging - skipping merge")
    dbutils.notebook.exit("No records in staging - skipping merge")

print(f"Staging records to process: {staging_count:,}")

# Step 1: Mark records as INACTIVE where data has CHANGED
print("\nStep 1: Marking CHANGED records as inactive...")

spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING {STAGING_TABLE} AS source
    ON target.circuit = source.circuit 
        AND target.utility_id = source.utility_id
        AND target.active = TRUE
    
    WHEN MATCHED AND (
        -- Only mark inactive if data has changed
        COALESCE(target.fvoltage, -999) != COALESCE(source.fvoltage, -999) OR
        COALESCE(target.fmaxhc, -999) != COALESCE(source.fmaxhc, -999) OR
        COALESCE(target.fminhc, -999) != COALESCE(source.fminhc, -999) OR
        COALESCE(target.refresh_since, 0) != COALESCE(source.refresh_since, 0) OR
        COALESCE(target.refresh_date, TIMESTAMP('1900-01-01 00:00:00')) != COALESCE(source.refresh_date, TIMESTAMP('1900-01-01 00:00:00')) OR
        COALESCE(target.shape_length, -999) != COALESCE(source.shape_length, -999)
    ) THEN
        UPDATE SET active = FALSE
""")

#print(f"Marked {inactive_count:,} CHANGED records as inactive")

# Step 2: Insert ONLY new records OR changed records (not unchanged)
print("\nStep 2: Inserting new and changed records...")

spark.sql(f"""
    INSERT INTO {SILVER_TABLE} (
        circuit,
        utility_id,
        fvoltage,
        fmaxhc,
        fminhc,
        refresh_since,
        refresh_date,
        shape_length,
        active,
        ingestion_time,
        source_file
    )
    SELECT
        stg.circuit,
        stg.utility_id,
        stg.fvoltage,
        stg.fmaxhc,
        stg.fminhc,
        stg.refresh_since,
        stg.refresh_date,
        stg.shape_length,
        TRUE as active,
        CURRENT_TIMESTAMP() as ingestion_time,
        stg.source_file
    FROM {STAGING_TABLE} stg
    LEFT JOIN {SILVER_TABLE} silver
    ON  silver.circuit       = stg.circuit
    AND silver.utility_id    = stg.utility_id
    AND silver.active        = TRUE
    AND COALESCE(silver.fvoltage, -999)      = COALESCE(stg.fvoltage, -999)
    AND COALESCE(silver.fmaxhc, -999)        = COALESCE(stg.fmaxhc, -999)
    AND COALESCE(silver.fminhc, -999)        = COALESCE(stg.fminhc, -999)
    AND COALESCE(silver.refresh_since, 0)    = COALESCE(stg.refresh_since, 0)
    AND COALESCE(silver.refresh_date, TIMESTAMP('1900-01-01')) = COALESCE(stg.refresh_date, TIMESTAMP('1900-01-01'))
    AND COALESCE(silver.shape_length, -999)  = COALESCE(stg.shape_length, -999)
    WHERE silver.circuit IS NULL 
    """)

"""
            -- And data is IDENTICAL (unchanged)
            AND COALESCE(silver.fvoltage, -999) = COALESCE(stg.fvoltage, -999)
            AND COALESCE(silver.fmaxhc, -999) = COALESCE(stg.fmaxhc, -999)
            AND COALESCE(silver.fminhc, -999) = COALESCE(stg.fminhc, -999)
            AND COALESCE(silver.refresh_since, 0) = COALESCE(stg.refresh_since, 0)
            AND COALESCE(silver.refresh_date, TIMESTAMP('1900-01-01 00:00:00')) = COALESCE(stg.refresh_date, TIMESTAMP('1900-01-01 00:00:00'))
            AND COALESCE(silver.shape_length, -999) = COALESCE(stg.shape_length, -999)
"""

ts_filter = f"CAST('{target_max_ts}' AS TIMESTAMP)" if target_max_ts else "TIMESTAMP('1900-01-01')"

inserted_count = spark.sql(f"""
    SELECT COUNT(circuit_id) 
    FROM {SILVER_TABLE} 
    WHERE ingestion_time > {ts_filter}
""").collect()[0][0]

print(f"Inserted {inserted_count:,} new/changed records")

# Step 3: Verify results
"""
print("\nStep 3: Verifying merge results...")

total_count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_TABLE}").collect()[0][0]
active_count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_TABLE} WHERE active = TRUE").collect()[0][0]

print(f"Total records in Silver: {total_count:,}")
print(f"Active records: {active_count:,}")
print(f"Inactive (historical): {total_count - active_count:,}")
"""

print("\n" + "="*80)
print("MERGE COMPLETED SUCCESSFULLY")
print("="*80)

