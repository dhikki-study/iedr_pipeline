# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

print("Libraries imported")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Table configuration
CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Build fully qualified table names
BRONZE_PLANNED_DER = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_planned_der"
STAGING_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der_stg"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_planned_der"

print("Table Configuration:")
print(f"   Bronze:  {BRONZE_PLANNED_DER}")
print(f"   Staging: {STAGING_TABLE}")
print(f"   Silver:  {SILVER_TABLE}")

# COMMAND ----------

def get_max_ingestion_timestamp():

    try:
        result = spark.sql(f"""
            SELECT MAX(ingestion_timestamp) as max_timestamp
            FROM {SILVER_TABLE}
        """).collect()[0]['max_timestamp']
        
        if result:
            print(f"Max ingestion timestamp in Silver: {result}")
            return result
        else:
            print("Silver table is empty - this is the first load")
            return None
            
    except Exception as e:
        print(f"Silver table doesn't exist yet - this is the first load")
        return None


# COMMAND ----------

def extract_and_unpivot_from_bronze(max_timestamp):

    print("\n" + "="*80)
    print("Extracting from Bronze")
    print("="*80)
    
    # Build WHERE clause for incremental filter
    if max_timestamp:
        where_clause = f"ingestion_timestamp > CAST('{max_timestamp}' AS TIMESTAMP)"
        print(f"Filter: ingestion_timestamp > {max_timestamp}")
    else:
        where_clause = "1=1"  # Get all records (first load)
        print("Filter: ALL records (first load)")
    
    # STEP 1: Extract ALL data from Bronze first
    print("\nStep 1: Extracting all data from Bronze...")
    df_bronze = spark.sql(f"""
        SELECT *
        FROM {BRONZE_PLANNED_DER}
        WHERE {where_clause}
    """)
    
    if df_bronze.isEmpty():
        print("No new records to process")
        return df_bronze.limit(0), 0
    
    # STEP 2: Split by utility
    print("\nStep 2: Splitting by utility...")
    df_utility1 = df_bronze.filter(col("utility_id") == "utility1")
    df_utility2 = df_bronze.filter(col("utility_id") == "utility2")
    
    utility1_count = df_utility1.count()
    utility2_count = df_utility2.count()
    
    print(f"utility1: {utility1_count:,} records (needs unpivot)")
    print(f"utility2: {utility2_count:,} records (pass through)")
    
    # STEP 3: Unpivot utility1 ONLY
    if utility1_count > 0:
        print("\nStep 3a: Unpivoting utility1 (15 DER columns → DER_TYPE, DER_VALUE)...")
        
        # Create temp view
        #df_utility1.createOrReplaceTempView("utility1_bronze_temp")
        
        # Unpivot using spark.sql with stack()
        df_utility1_unpivoted = df_utility1.selectExpr(
            # Project identification
            "project_id",
            "queue_request_id",
            "project_circuit_id",
            
            # Location
            "der_interconn_loc",

            
            # Project details
            "utility_id",
            "project_type",
            "nameplate_rating",
            "inverter_nameplate_rating",
            "Hybrid",
            
            # Temporal information
            "in_service_date",
            "completion_date",
                
            # Status information
            "project_status",
            "status_rationale",
            "queue_position",
            "total_mw_substation",
            
            # UNPIVOT: Convert 15 DER columns into DER_TYPE and DER_VALUE
            """
            stack(14,
                'SolarPV', SolarPV,
                'EnergyStorageSystem', EnergyStorageSystem,
                'Wind', Wind,
                'MicroTurbine', MicroTurbine,
                'SynchronousGenerator', SynchronousGenerator,
                'InductionGenerator', InductionGenerator,
                'FarmWaste', FarmWaste,
                'FuelCell', FuelCell,
                'CombinedHeatandPower', CombinedHeatandPower,
                'GasTurbine', GasTurbine,
                'Hydro', Hydro,
                'InternalCombustionEngine', InternalCombustionEngine,
                'SteamTurbine', SteamTurbine,
                'Other', Other
            ) as (DER_TYPE, DER_VALUE)
            """,
            
            # Metadata
            "source_file",
            "ingestion_timestamp"
        )

        # Filter: Only keep rows where DER_VALUE > 0
        df_utility1_filtered = df_utility1_unpivoted.filter(
            (col("DER_VALUE").isNotNull()) & (col("DER_VALUE") > 0)
        )
        
        utility1_unpivoted_count = df_utility1_filtered.count()
        print(f"utility1 unpivoted: {utility1_count:,} → {utility1_unpivoted_count:,} rows (filtered DER_VALUE > 0)")
    else:
        df_utility1_filtered = df_utility1.limit(0)  # Empty dataframe
        utility1_unpivoted_count = 0
        print("No utility1 records to unpivot")
    
    # STEP 4: Process utility2 (pass through)
    if utility2_count > 0:
        print("\nStep 3b: Processing utility2 (pass through - already in correct format)...")
        
        # utility2 already has DER_TYPE and DER_VALUE columns
        df_utility2_processed = df_utility2.select(
            # Project identification
            "project_id",
            "queue_request_id",
            "project_circuit_id",  # utility2 uses circuit_id
            
            # Location
            "der_interconn_loc",
            
            # Project details
            "utility_id",
            "project_type",
            "nameplate_rating",
            "inverter_nameplate_rating",
            "Hybrid",
            
            # DER Type (already in long format)
            F.col("project_type").alias("DER_TYPE"),
            F.col("nameplate_rating").alias("DER_VALUE"),
            
            # Temporal information
            "in_service_date",
            "completion_date",
            
            # Status information
            "project_status",
            "status_rationale",
            "queue_position",
            
            # Substation information
            "total_mw_substation",
            
            # Metadata
            "source_file",
            "ingestion_timestamp"
        )
        
        print(f"utility2 processed: {utility2_count:,} records (no unpivot needed)")
    else:
        df_utility2_processed = df_utility2.limit(0)  # Empty dataframe
        print("No utility2 records to process")
    
    # STEP 5: Combine utility1 (unpivoted) + utility2 (pass through)
    print("\nStep 4: Combining utility1 and utility2...")
    
    df_utility1_filtered = df_utility1_filtered.select(df_utility2_processed.columns)

    df_combined = df_utility1_filtered.union(df_utility2_processed)
    final_count = utility1_unpivoted_count + utility2_count
    print(f"Combined: {final_count:,} total records")
     

    return df_combined, final_count


# COMMAND ----------

def load_to_staging(df, record_count):
    """
    Load unpivoted records into silver_planned_der_stg using overwrite mode.
    """
    if record_count == 0:
        print("No records to load - skipping staging")
        return False
    
    print("\n" + "="*80)
    print("Loading records to Staging")
    print("="*80)
    

    print(f"Overwriting {STAGING_TABLE} with {record_count:,} records...")
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(STAGING_TABLE)
    
    print(f"Loaded {record_count:,} records to staging")
    
 
    return True


# COMMAND ----------

def run_bronze_to_staging_planned_der():
    """
    Main orchestration: Bronze → Staging for planned DER
    """
    print("\n" + "="*80)
    print("STARTING BRONZE → STAGING EXTRACTION (PLANNED DER)")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Get max timestamp from Silver
        print("\nStep 1: Getting max ingestion timestamp from Silver...")
        max_timestamp = get_max_ingestion_timestamp()
        
        # Step 2: Extract and unpivot from Bronze
        print("\nStep 2: Extracting and unpivoting from Bronze...")
        df_final, record_count = extract_and_unpivot_from_bronze(max_timestamp)
        
        # Step 3: Load to staging
        print("\nStep 3: Loading to staging table...")
        loaded = load_to_staging(df_final, record_count)
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Max timestamp in Silver: {max_timestamp if max_timestamp else 'N/A (first load)'}")
        print(f"Records loaded to Staging: {record_count:,}" if loaded else "No records loaded")
        print("="*80)
        
        if loaded:
            print("\nSUCCESS: Staging table ready for MERGE into Silver")
        else:
            print("\nSKIPPED: No new records to process")
        
        return loaded
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# Execute

staging_loaded = run_bronze_to_staging_planned_der()