# Databricks notebook source


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "workspace"
SILVER_SCHEMA = "silver"

# Build fully qualified table names
STAGING_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der_stg"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der"

print("\n" + "="*80)
print("MERGING STAGING → SILVER (Type 2 SCD)")
print("="*80)

# Check if staging has data
staging_count = spark.sql(f"SELECT COUNT(*) FROM {STAGING_TABLE}").collect()[0][0]

if staging_count == 0:
    print("No records in staging - skipping merge")
    dbutils.notebook.exit("No records in staging - skipping merge")

print(f"Staging records to process: {staging_count:,}")

# Step 1: Mark records as INACTIVE where data has CHANGED
print("\nStep 1: Marking CHANGED records as inactive...")

spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING {STAGING_TABLE} AS source
    ON target.project_id = source.project_id 
        AND target.DER_TYPE = source.DER_TYPE
        AND target.active = TRUE
    
    WHEN MATCHED AND (
        -- Check if any data has changed
        COALESCE(target.queue_request_id, '') != COALESCE(source.queue_request_id, '') OR
        COALESCE(target.project_circuit_id, '') != COALESCE(source.project_circuit_id, '') OR
        COALESCE(target.der_interconn_loc, '') != COALESCE(source.der_interconn_loc, '') OR
        COALESCE(target.project_type, '') != COALESCE(source.project_type, '') OR
        COALESCE(target.nameplate_rating, -999) != COALESCE(source.nameplate_rating, -999) OR
        COALESCE(target.inverter_nameplate_rating, -999) != COALESCE(source.inverter_nameplate_rating, -999) OR
        COALESCE(target.Hybrid, '') != COALESCE(source.Hybrid, '') OR
        COALESCE(target.DER_VALUE, -999) != COALESCE(source.DER_VALUE, -999) OR
        COALESCE(target.in_service_date, TIMESTAMP('1900-01-01 00:00:00')) != COALESCE(source.in_service_date, TIMESTAMP('1900-01-01 00:00:00')) OR
        COALESCE(target.completion_date, TIMESTAMP('1900-01-01 00:00:00')) != COALESCE(source.completion_date, TIMESTAMP('1900-01-01 00:00:00')) OR
        COALESCE(target.project_status, '') != COALESCE(source.project_status, '') OR
        COALESCE(target.status_rationale, '') != COALESCE(source.status_rationale, '') OR
        COALESCE(target.queue_position, '') != COALESCE(source.queue_position, '') OR
        COALESCE(target.total_mw_substation, -999) != COALESCE(source.total_mw_substation, -999)
    ) THEN
        UPDATE SET active = FALSE
""")


# Step 2: Insert ONLY new records OR changed records (not unchanged)
print("\nStep 2: Inserting new and changed records...")

spark.sql(f"""
    INSERT INTO {SILVER_TABLE} (
        project_id,
        queue_request_id,
        project_circuit_id,
        der_interconn_loc,
        utility_id,
        project_type,
        nameplate_rating,
        inverter_nameplate_rating,
        Hybrid,
        DER_TYPE,
        DER_VALUE,
        in_service_date,
        completion_date,
        project_status,
        status_rationale,
        queue_position,
        total_mw_substation,
        active,
        ingestion_timestamp,
        source_file
    )
    SELECT
        stg.project_id,
        stg.queue_request_id,
        stg.project_circuit_id,
        stg.der_interconn_loc,
        stg.utility_id,
        stg.project_type,
        stg.nameplate_rating,
        stg.inverter_nameplate_rating,
        stg.Hybrid,
        stg.DER_TYPE,
        stg.DER_VALUE,
        stg.in_service_date,
        stg.completion_date,
        stg.project_status,
        stg.status_rationale,
        stg.queue_position,
        stg.total_mw_substation,
        TRUE as active,
        CURRENT_TIMESTAMP() as ingestion_timestamp,
        stg.source_file
    FROM {STAGING_TABLE} stg
    LEFT JOIN {SILVER_TABLE} silver
        ON silver.project_id = stg.project_id
        AND silver.utility_id    = stg.utility_id
            AND silver.DER_TYPE = stg.DER_TYPE
            AND silver.active = TRUE
            -- And data is IDENTICAL (unchanged)
            AND COALESCE(silver.queue_request_id, '') = COALESCE(stg.queue_request_id, '')
            AND COALESCE(silver.project_circuit_id, '') = COALESCE(stg.project_circuit_id, '')
            AND COALESCE(silver.der_interconn_loc, '') = COALESCE(stg.der_interconn_loc, '')
            AND COALESCE(silver.project_type, '') = COALESCE(stg.project_type, '')
            AND COALESCE(silver.nameplate_rating, -999) = COALESCE(stg.nameplate_rating, -999)
            AND COALESCE(silver.inverter_nameplate_rating, -999) = COALESCE(stg.inverter_nameplate_rating, -999)
            AND COALESCE(silver.Hybrid, '') = COALESCE(stg.Hybrid, '')
            AND COALESCE(silver.DER_VALUE, -999) = COALESCE(stg.DER_VALUE, -999)
            AND COALESCE(silver.in_service_date, TIMESTAMP('1900-01-01 00:00:00')) = COALESCE(stg.in_service_date, TIMESTAMP('1900-01-01 00:00:00')) 
            AND COALESCE(silver.completion_date, TIMESTAMP('1900-01-01 00:00:00')) = COALESCE(stg.completion_date, TIMESTAMP('1900-01-01 00:00:00')) 
            AND COALESCE(silver.project_status, '') = COALESCE(stg.project_status, '')
            AND COALESCE(silver.status_rationale, '') = COALESCE(stg.status_rationale, '')
            AND COALESCE(silver.queue_position, '') = COALESCE(stg.queue_position, '')
            AND COALESCE(silver.total_mw_substation, -999) = COALESCE(stg.total_mw_substation, -999)
    WHERE silver.project_id IS NULL 
    
    """)


# Step 3: Verify results
print("\nStep 3: Verifying merge results...")

total_count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_TABLE}").collect()[0][0]
active_count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_TABLE} WHERE active = TRUE").collect()[0][0]

print(f"Total records in Silver: {total_count:,}")
print(f"Active records: {active_count:,}")
print(f"Inactive (historical): {total_count - active_count:,}")

print("\n" + "="*80)
print("MERGE COMPLETED SUCCESSFULLY")
print("="*80)

