# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F
from datetime import datetime

BRONZE_TBLE="workspace.bronze.bronze_circuits"
STAGING_TABLE="workspace.silver.silver_circuits_stg"
SILVER_TABLE="workspace.silver.silver_circuits"

# COMMAND ----------

def get_max_ingestion_timestamp():
    result = spark.sql(f"""
        SELECT MAX(ingestion_time) as max_timestamp
        FROM {SILVER_TABLE}
    """).collect()[0]['max_timestamp']
    
    if result:
        print(f"Max ingestion timestamp in Silver: {result}")
        return result
    else:
        print("Silver table is empty - this is the first load")
        return None

#get_max_ingestion_timestamp()

# COMMAND ----------

def extract_incremental_from_bronze(max_timestamp):

    print("Extracting incremental records from Bronze")
    # Build the WHERE clause
    if max_timestamp:
        where_clause = f"ingestion_timestamp > CAST('{max_timestamp}' AS TIMESTAMP)"
        print(f"Filter: ingestion_timestamp > {max_timestamp}")
    else:
        where_clause = "1=1"  # Get all records (first load)
        print("Filter: ALL records (first load)")

    # Extract from Bronze with column mapping
    df_incremental = spark.sql(f"""
        SELECT
            circuit,
            utility_id,
            fvoltage,
            fmaxhc,
            fminhc,
            refresh_since,
            refresh_date,
            shape_length,
            ingestion_timestamp as ingestion_time,
            source_file
        FROM {BRONZE_TBLE}
        WHERE {where_clause}
    """)
    
    # Count records
    record_count = df_incremental.count()
    print(f"✅ Found {record_count:,} incremental records")
    
    return df_incremental, record_count


# COMMAND ----------

def load_to_staging(df, record_count):

    if record_count == 0:
        print("No records to load - skipping staging")
        return False
    
    print("\n" + "="*80)
    print("Loading records to Staging with Aggregation")
    print("="*80)
    
    # Split by utility
    df_utility1 = df.filter(F.col("utility_id") == "utility1")
    df_utility2 = df.filter(F.col("utility_id") == "utility2")
    
    #utility1_count = df_utility1.count()
    #utility2_count = df_utility2.count()
    
    #print(f"utility1 records (before agg): {utility1_count:,}")
    #print(f"utility2 records (no agg):     {utility2_count:,}")
    
    # Aggregate utility1 to circuit level
    df_utility1_agg = df_utility1.groupBy("circuit", "utility_id", "source_file").agg(
            F.round(F.avg("fvoltage"),2).alias("fvoltage"),
            F.max("fmaxhc").alias("fmaxhc"),
            F.min("fminhc").alias("fminhc"),
            F.max("refresh_since").alias("refresh_since"),
            F.max("refresh_date").alias("refresh_date"),
            F.round(F.avg("shape_length"),2).alias("shape_length"),
            F.max("ingestion_time").alias("ingestion_time")
        )
        
        
    df_utility1_agg = df_utility1_agg.select(df_utility2.columns)

    # Combine utility1 (aggregated) + utility2 (pass-through)
    df_combined = df_utility1_agg.union(df_utility2)
    #print(f"\nCombined: {df_combined.count():,} total circuits")
    
    
    # Overwrite staging table
    print(f"\nOverwriting {STAGING_TABLE}...")
    df_combined.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(STAGING_TABLE)
    
    final_count = spark.table(STAGING_TABLE).count()
    print(f"Loaded {final_count:,} circuits to staging")
    
    return True

# COMMAND ----------


# Initialize Spark
spark = SparkSession.builder.appName("SilverStgIngestion").getOrCreate()
print("Spark session initialized")

print("\n" + "="*80)
print("STARTING BRONZE → STAGING EXTRACTION")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

try:
    # Step 1: Get max timestamp from Silver
    print("\nStep 1: Getting max ingestion timestamp from Silver...")
    max_timestamp = get_max_ingestion_timestamp()
    
    # Step 2: Extract incremental records from Bronze
    print("\nStep 2: Extracting incremental records from Bronze...")
    df_incremental, record_count = extract_incremental_from_bronze(max_timestamp)
    
    # Step 3: Load to staging
    print("\nStep 3: Loading to staging table...")
    loaded = load_to_staging(df_incremental, record_count)
    
    # Summary
    print("\n" + "="*80)
    print("EXTRACTION SUMMARY")
    print("="*80)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Max timestamp in Silver: {max_timestamp if max_timestamp else 'N/A (first load)'}")
    print(f"Records extracted from Bronze: {record_count:,}")
    print(f"Records loaded to Staging: {record_count:,}" if loaded else "No records loaded")
    print("="*80)
    
    if loaded:
        print("\nSUCCESS: Staging table ready for MERGE into Silver")
    else:
        print("\nSKIPPED: No new records to process")
    
except Exception as e:
    print("\n" + "="*80)
    print("ERROR IN EXTRACTION PROCESS")
    print("="*80)
    print(f"Error: {str(e)}")
    raise




