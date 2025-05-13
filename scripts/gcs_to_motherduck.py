import duckdb
import pyarrow.fs as fs
import yaml
import time
import math

# --- Load config ---
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

BUCKET = config["gcs"]["bucket_name"]
PREFIX = config["gcs"]["prefix"]
DB_NAME = config["motherduck"]["database_name"]
TABLE = config["motherduck"]["target_table"]
BATCH_SIZE = config["processing"]["files_per_batch"]
NUM_FILES = config["processing"]["num_files"]

# --- List parquet files from GCS ---
def list_parquet_files(bucket, prefix):
    gcs = fs.GcsFileSystem()
    selector = fs.FileSelector(f"{bucket}/{prefix}", recursive=True)
    files = gcs.get_file_info(selector)
    print (f"Found {len(files)} Parquet files.")
    return [
        f"gs://{f.path}" for f in files
        if f.is_file and f.path.endswith(".parquet")
    ]
    
# --- Connect to MotherDuck ---
def connect():
    try:
        con = duckdb.connect(f"md:{DB_NAME}")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        print("✅ Connected to MotherDuck")
        return con
    except Exception as e:
        raise RuntimeError(f"❌ Connection failed: {e}")

# --- Execute COPY FROM for a batch of files ---# --- Execute COPY FROM for a batch of files ---
def copy_batch(con, table, batch):
    if not batch:
        print("Warning: copy_batch called with empty list. Skipping.")
        return True

    files_sql = "[" + ", ".join([f"'{f}'" for f in batch]) + "]"
    copy_query = f"COPY {table} FROM read_parquet({files_sql}) (FORMAT PARQUET);"             

    try:
        con.execute(copy_query)
        return True
    except Exception as e:
        # The error message indicates the SQL syntax was wrong *before* even trying to access files
        print(f"❌ COPY failed: {e}") # The Parser Error happens here
        # Logging batch[0] is still misleading for a syntax error
        print(f"❌ Review generated SQL syntax (first 200 chars): {copy_query[:200]}")
        return False

# --- Main orchestration ---
def main():
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["gcs"]["credentials"]
    con = connect()
    files = list_parquet_files(BUCKET, PREFIX)[:NUM_FILES]
    total_batches = math.ceil(len(files) / BATCH_SIZE)

    print(f"🚀 Ingesting {len(files)} files into {TABLE} in {total_batches} batches...")
    start = time.time()

    for i in range(total_batches):
        batch = files[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]
        print(f"\n Batch {i + 1}/{total_batches}: {len(batch)} files")

        if not copy_batch(con, TABLE, batch):
            print("⛔ Aborting pipeline due to COPY failure.")
            break

    con.close()
    print(f"\n✅ Done in {time.time() - start:.2f}s")

if __name__ == "__main__":
    main()