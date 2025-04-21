import duckdb
from google.cloud import storage
import pyarrow.fs as fs
import os
import math
import time
import yaml 

# --- Configuration Loading ---
CONFIG_FILE = 'config.yaml'

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# --- Config values ---
gcs_bucket = config["gcs"]["bucket_name"]
gcs_prefix = config["gcs"]["prefix"]
motherduck_db = config["motherduck"]["database_name"]
motherduck_table = config["motherduck"]["target_table"]
batch_size = config["processing"]["files_per_batch"]

# --- Task Functions ---
def list_bucket_files(gcs_bucket, gcs_prefix):
    """
    Lists all Parquet files in a GCS bucket, given a prefix, into a list.
    """
    gcs = fs.GcsFileSystem() 

    # --- LIST PARQUET FILES ---
    print(f"Listing Parquet files in {gcs_bucket}/{gcs_prefix}...")
    parquet_files = []
    try:
        selector = fs.FileSelector(gcs_bucket + "/" + gcs_prefix.strip('/'), recursive=True)
        files = gcs.get_file_info(selector)
        parquet_files = [
            f"gs://{f.path}" 
            for f in files if f.is_file and f.path.endswith(".parquet")
        ]
        print(f"Found {len(parquet_files)} Parquet files.")
    except Exception as e:
        print(f"ERROR: Failed to list Parquet files: {e}")
    
    return parquet_files

def connect_to_motherduck():
    """Connects to MotherDuck and prepares the connection."""
    try:
        con = duckdb.connect(f"md:{motherduck_db}")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        print(f"✅ Connected to MotherDuck.{con}")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MotherDuck: {e}")
    return con
    

def execute_copy_batch(connection, target_table, batch_files):
    """Executes a single COPY command for a batch of files."""
 
    # Create the SQL list string: ('file1', 'file2', ...)
    file_list_sql = "(" + ", ".join([f"'{f}'" for f in batch_files]) + ")"
    copy_sql = f"COPY {target_table} FROM {file_list_sql} (FORMAT PARQUET);"

    try:
        print(f"Executing COPY for {len(batch_files)} files...")
        connection.execute(copy_sql)
        return True # Indicate success
    except Exception as e:
        print(f"ERROR executing COPY command: {e}")
        # Print first file for context without overwhelming logs
        print(f"Failed SQL roughly: COPY {target_table} FROM ('{batch_files[0]}', ...) (FORMAT PARQUET);")
        return False # Indicate failure

# --- Main Orchestration Logic ---
def main():
    """Main function to orchestrate the batch ingestion process."""
    
    motherduck_token = os.environ.get("motherduck_token")
    
    # 1. List Parquet files from bucket
    bucket_files = list_bucket_files(gcs_bucket, gcs_prefix)
    
    # 2. Connect to MotherDuck
    connection = connect_to_motherduck()
    if connection is None:
        print("Exiting due to MotherDuck connection error.")
        exit(1)

    # 3. Process batches
    files_per_batch = config['processing']['files_per_batch']
    target_table = config['motherduck']['target_table']
    num_files = config['processing']['num_files']
    
    num_batches = math.ceil(num_files / files_per_batch)

    print(f"\nStarting ingestion into table '{target_table}' in {num_batches} batches of up to {files_per_batch} files each.")
    total_start_time = time.time()
    batches_succeeded = 0
    batches_failed = 0

    try: 
        for i in range(num_batches):
            batch_start_time = time.time()
            start_index = i * files_per_batch
            end_index = min((i + 1) * files_per_batch, num_files)
            batch_files = bucket_files[i:i + files_per_batch]

            print(f"\n--- Processing Batch {i+1}/{num_batches} ---")
            success = execute_copy_batch(connection, target_table, batch_files)

            if success:
                batches_succeeded += 1
                batch_duration = time.time() - batch_start_time
                print(f"Batch {i+1} completed successfully in {batch_duration:.2f} seconds.")
            else:
                batches_failed += 1
                print(f"Batch {i+1} failed. Stopping script.")
                break # Stop processing further batches on failure

    finally: # Ensure connection is closed even if errors occur
        if connection:
            connection.close()
            print("\nMotherDuck connection closed.")

    # 4. Final Summary
    total_duration = time.time() - total_start_time
    print("\n--- Ingestion Summary ---")
    print(f"Batches Attempted: {batches_succeeded + batches_failed}/{num_batches}")
    print(f"Batches Succeeded: {batches_succeeded}")
    print(f"Batches Failed:    {batches_failed}")
    print(f"Total time: {total_duration:.2f} seconds.")

    if batches_failed > 0:
        exit(1) # Exit with error code if any batch failed


if __name__ == "__main__":
    main()
