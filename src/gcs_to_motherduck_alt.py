import yaml
import duckdb
import pyarrow.fs as fs
import os

# --- LOAD CONFIG ---
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# --- EXTRACT CONFIG VALUES ---
gcs_bucket = config["gcs"]["bucket_name"]
gcs_prefix = config["gcs"]["prefix"]
motherduck_db = config["motherduck"]["database_name"]
motherduck_table = config["motherduck"]["target_table"]
batch_size = config["processing"]["files_per_batch"]


# --- INIT GCS FILESYSTEM ---
gcs = fs.GcsFileSystem()

# --- LIST PARQUET FILES ---
files = gcs.get_file_info(fs.FileSelector(gcs_bucket + "/" + gcs_prefix, recursive=True))
parquet_files = [f.path for f in files if f.path.endswith(".parquet")]

# --- CONNECT TO MOTHERDUCK ---
try:
    con = duckdb.connect(f"md:{motherduck_db}")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
except Exception as e:
    raise ConnectionError(f"Failed to connect to MotherDuck: {e}")

# --- PROCESS FILES IN BATCHES ---
for i in range(0, len(parquet_files), batch_size):
    batch = parquet_files[i:i + batch_size]
    print(f"🔄 Processing batch {i // batch_size + 1} with {len(batch)} files")

    for file_path in batch:
        print(f"📄 Loading: {file_path}")
        full_path = f"gcs://{file_path}"

        # Insert data directly from GCS into MotherDuck table
        con.execute(f"""
            CREATE TABLE {motherduck_table} AS
            COPY SELECT * FROM read_parquet('{full_path}')
        """)

print("✅ All batches processed and inserted into MotherDuck.")