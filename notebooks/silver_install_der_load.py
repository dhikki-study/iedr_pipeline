# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F
from datetime import datetime



STAGING_TABLE="workspace.silver.silver_installed_der_stg"
SILVER_TABLE="workspace.silver.silver_installed_der"

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
        COALESCE(target.project_circuit_id, '') != COALESCE(source.project_circuit_id, '') OR
        COALESCE(target.der_interconn_loc, '') != COALESCE(source.der_interconn_loc, '') OR
        COALESCE(target.street_address, '') != COALESCE(source.street_address, '') OR
        COALESCE(target.project_type, '') != COALESCE(source.project_type, '') OR
        COALESCE(target.nameplate_rating, -999) != COALESCE(source.nameplate_rating, -999) OR
        COALESCE(target.Hybrid, '') != COALESCE(source.Hybrid, '') OR
        COALESCE(target.DER_VALUE, -999) != COALESCE(source.DER_VALUE, -999) OR
        COALESCE(target.total_charges_cesir, -999) != COALESCE(source.total_charges_cesir, -999) OR
        COALESCE(target.total_charges_construction, -999) != COALESCE(source.total_charges_construction, -999) OR
        COALESCE(target.cesir_est, -999) != COALESCE(source.cesir_est, -999) OR
        COALESCE(target.system_upgrade_est, -999) != COALESCE(source.system_upgrade_est, -999) OR
        COALESCE(target.interconnection_cost, -999) != COALESCE(source.interconnection_cost, -999)
    ) THEN
        UPDATE SET active = FALSE
""")


print(f"Marked records as inactive")

# Step 2: Insert ONLY new records OR changed records (not unchanged)
print("\nStep 2: Inserting new and changed records...")

spark.sql(f"""
    INSERT INTO {SILVER_TABLE} (
        project_id,
        project_circuit_id,
        der_interconn_loc,
        street_address,
        utility_id,
        project_type,
        nameplate_rating,
        Hybrid,
        DER_TYPE,
        DER_VALUE,
        total_charges_cesir,
        total_charges_construction,
        cesir_est,
        system_upgrade_est,
        interconnection_cost,
        active,
        ingestion_timestamp,
        source_file
    )
    SELECT
        stg.project_id,
        stg.project_circuit_id,
        stg.der_interconn_loc,
        stg.street_address,
        stg.utility_id,
        stg.project_type,
        stg.nameplate_rating,
        stg.Hybrid,
        stg.DER_TYPE,
        stg.DER_VALUE,
        stg.total_charges_cesir,
        stg.total_charges_construction,
        stg.cesir_est,
        stg.system_upgrade_est,
        stg.interconnection_cost,
        TRUE as active,
        CURRENT_TIMESTAMP() as ingestion_timestamp,
        stg.source_file
    FROM {STAGING_TABLE} stg
    LEFT JOIN {SILVER_TABLE} silver
        ON silver.project_id = stg.project_id
        AND silver.utility_id    = stg.utility_id
            AND silver.DER_TYPE = stg.DER_TYPE
            AND silver.active        = TRUE
            -- And data is IDENTICAL (unchanged)
            AND COALESCE(silver.project_circuit_id, '') = COALESCE(stg.project_circuit_id, '')
            AND COALESCE(silver.der_interconn_loc, '') = COALESCE(stg.der_interconn_loc, '')
            AND COALESCE(silver.street_address, '') = COALESCE(stg.street_address, '')
            AND COALESCE(silver.project_type, '') = COALESCE(stg.project_type, '')
            AND COALESCE(silver.nameplate_rating, -999) = COALESCE(stg.nameplate_rating, -999)
            AND COALESCE(silver.Hybrid, '') = COALESCE(stg.Hybrid, '')
            AND COALESCE(silver.DER_VALUE, -999) = COALESCE(stg.DER_VALUE, -999)
            AND COALESCE(silver.total_charges_cesir, -999) = COALESCE(stg.total_charges_cesir, -999)
            AND COALESCE(silver.total_charges_construction, -999) = COALESCE(stg.total_charges_construction, -999)
            AND COALESCE(silver.cesir_est, -999) = COALESCE(stg.cesir_est, -999)
            AND COALESCE(silver.system_upgrade_est, -999) = COALESCE(stg.system_upgrade_est, -999)
            AND COALESCE(silver.interconnection_cost, -999) = COALESCE(stg.interconnection_cost, -999)
    WHERE silver.project_id IS NULL 
    
""")


print("\n" + "="*80)
print("MERGE COMPLETED SUCCESSFULLY")
print("="*80)
