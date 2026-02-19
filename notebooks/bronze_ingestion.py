# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Ingestion

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F
from datetime import datetime

LANDING_BASE_PATH = "/Volumes/workspace/default/iedr_csv/landing/"
ARCHIVE_BASE_PATH = "/Volumes/workspace/default/iedr_csv/archive/"

print("Configuration loaded")
print(f"Landing path: {LANDING_BASE_PATH}")
print(f"Archive path: {ARCHIVE_BASE_PATH}")


# COMMAND ----------

# DBTITLE 1,Get config for file (fixed)
def get_config_for_file(filenm: str):
    """Get column mappings from config table for a specific file."""
    config_df = spark.sql(f"""
        SELECT source_column_name, target_column_name, data_type
        FROM workspace.bronze.config_column_mapping
        WHERE filename = '{filenm}'
    """)
    
    config_rows = config_df.collect()
    
    if len(config_rows) == 0:
        raise ValueError(f"No config found for {filenm}")
    
    # Create dictionary for renaming (matches your code structure)
    column_mappings = {row.source_column_name: row.target_column_name 
                      for row in config_rows}
    
    # Return both: dict for renaming, rows for type casting
    return column_mappings, config_rows


#get_config_for_file('utility1_circuits.csv')

# COMMAND ----------

def get_target_table(filename: str) -> str:
    if 'circuits' in filename:
        return 'workspace.bronze.bronze_circuits'
    elif 'install_der' in filename:
        return 'workspace.bronze.bronze_installed_der'
    elif 'planned_der' in filename:
        return 'workspace.bronze.bronze_planned_der'
    else:
        raise ValueError(f"Cannot determine target table for {filename}")

#print(get_target_table('utility1_circuits.csv'))

# COMMAND ----------

def get_utility_id(filename: str) -> str:
    return filename.split('_')[0]

#print(get_utility_id('utility1_circuits.csv'))


# COMMAND ----------

def archive_file(source_path: str, filename: str):
    """Move file to archive with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_dir = f"{ARCHIVE_BASE_PATH}/"
    archive_file_path = f"{archive_dir}{filename}_{timestamp}"
    source_file=f"{source_path}"
    
    
    # Move file
    dbutils.fs.mv(source_file, archive_file_path)
    
    return archive_file_path

#print(archive_file('/Volumes/workspace/default/iedr_csv/landing/', 'utility1_circuits.csv'))

# COMMAND ----------

def process_file(file_path: str, filename: str):

    print(f"Processing: {filename}")
    print(f"{'='*80}")
    
    try:
        # Step 1: Get column mappings from config (WITH TYPES!)
        print("\nStep 1: Getting config...")
        column_mappings, config_rows = get_config_for_file(filename)  # ← Returns both!
        print(f"Found {len(column_mappings)} column mappings")
        
        # Step 2: Get target table
        print("\nStep 2: Determining target table...")
        target_table = get_target_table(filename)
        print(f"Target: {target_table}")
        
        # Step 3: Read CSV
        print("\nStep 3: Reading CSV...")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        #row_count = df.count()
        #print(f"Read {row_count} rows")
        
         # Step 4: Renaming and casting using config...
        print("\nStep 4: Renaming and casting using config...")
        for row in config_rows:
            source_col = row.source_column_name
            target_col = row.target_column_name
            data_type = row.data_type
            
            if source_col in df.columns:
                # Rename and cast in one step
                if data_type.lower() == 'timestamp_typ2':
                    # Special handling for timestamp - use to_timestamp with format
                    df = df.withColumn(
                        target_col,
                        F.expr(f"try_cast(replace({source_col}, '/', '-') as timestamp)")
                    )
                else:
                    # Regular try_cast for other data types (handles "NULL" strings)
                    df = df.withColumn(
                        target_col,
                        F.expr(f"try_cast({source_col} as {data_type})")
                    )
        
        print(f"Renamed and cast {len(config_rows)} columns")
        
        #display(df)
        
        # STEP 4.1: Get schema from table
        print("\nStep 4.1: Getting Bronze table schema...")
        

        bronze_schema = spark.table(target_table).schema
        metadata_cols = ['utility_id', 'ingestion_timestamp', 'source_file']
        bronze_columns = [f.name for f in bronze_schema.fields 
                        if f.name not in metadata_cols]
        type_lookup = {f.name: f.dataType for f in bronze_schema.fields}
        print(f"Bronze table has {len(bronze_columns)} columns")


        # STEP 4.2: Add missing columns as NULL
        print("\nStep 4.2: Adding missing columns as NULL...")
        
        missing_cols = set(bronze_columns) - set(df.columns)
        
        for col_name in missing_cols:
            if col_name in type_lookup:
                df = df.withColumn(col_name, lit(None).cast(type_lookup[col_name]))
            else:
                df = df.withColumn(col_name, lit(None))
        
        if missing_cols:
            print(f"Added {len(missing_cols)} missing columns as NULL")
        else:
            print(f"No missing columns")
        
        # Reorder columns to match Bronze schema
        if bronze_columns:
            df = df.select(bronze_columns)

        # Step 5: Add metadata
        print("\nStep 5: Adding metadata...")
        utility_id = get_utility_id(filename)
        df = df.withColumn("utility_id", lit(utility_id))
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        df = df.withColumn("source_file", lit(filename))
        print(f"Added metadata (utility_id={utility_id})")
        
        #display(df)
    
        # Step 6: Load to Bronze
        print(f"\nStep 6: Loading to {target_table}...")
        df.write \
          .format("delta") \
          .mode("append") \
          .saveAsTable(target_table)
        #print(f"Loaded {row_count} rows ")
        


        # Step 7: Archive file
        print("\nStep 7: Archiving file...")
        archive_path = archive_file(file_path, filename)
        print(f"Archived to: {archive_path}")
        

        
        print(f"\nSUCCESS: {filename} processed completely")
        return True, None

    except Exception as e:
        error_msg = str(e)
        print(f"\n ERROR: {error_msg}")
        return False, 0, error_msg
    
#process_file('dbfs:/Volumes/workspace/default/iedr_csv/landing/utility2_planned_der.csv/','utility2_planned_der.csv')


# COMMAND ----------

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
print("Spark session initialized")

print("Starting Bronze Layer Ingestion")
print("Started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print()

# Get all files in landing zone
try:
    all_files = dbutils.fs.ls(LANDING_BASE_PATH)
except Exception as e:
    print(f"Error accessing landing zone: {e}")
    dbutils.notebook.exit("Failed to access landing zone")

# Filter for CSV files
csv_files = [f for f in all_files if f.name.endswith('.csv')]

if not csv_files:
    print("No CSV files found in landing zone")
    print(f"Place CSV files in: {LANDING_BASE_PATH}")
    dbutils.notebook.exit("No files to process")

print(f"Found {len(csv_files)} CSV file(s)")


# Process each file
for file_info in csv_files:
    print("path and file:",file_info.path,file_info.name)
    success, error = process_file(file_info.path, file_info.name)
    if success:
        print(f"Processed {file_info.name}")
    else:
        print(f"Error processing {file_info.name}: {error}")
        raise Exception(f"Error processing {file_info.name}: {error}")



# COMMAND ----------

