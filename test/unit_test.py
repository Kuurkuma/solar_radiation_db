from google.cloud import storage
import pyarrow.fs as fs
import yaml 

from scripts.gcs_to_motherduck import list_bucket_files, connect_to_motherduck, copy_batch

# --- Configuration Loading ---
CONFIG_FILE = 'config.yaml'

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# --- Config values ---
gcs_bucket = config["gcs"]["bucket_name"]
gcs_prefix = config["gcs"]["prefix"]

print(f"Running unit test for gcs_to_motherduck.py: {list_bucket_files}...")

def main():
    #list_bucket_files(gcs_bucket, gcs_prefix)
    connect_to_motherduck()
    # copy_batch(con, table, batch)
    
if __name__ == "__main__": 
    main()