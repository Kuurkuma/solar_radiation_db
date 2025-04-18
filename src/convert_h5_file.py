import h5py
import pandas as pd
import yaml

# Loads configuration from a YAML file.
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)


def convert_h5_to_parquet(h5_file_path, parquet_file_path):
    with h5py.File(h5_file_path, "r") as f:
        df = pd.DataFrame(f["data"])
    df.to_parquet(parquet_file_path)
    
