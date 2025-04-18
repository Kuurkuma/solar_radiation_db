import pandas as pd
import yaml
import h5py
from rex import NSRDBX # package to import h5file


# Loads configuration from a YAML file.
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

nsrdb_file = config['aws_hdf5']['full_disc_irradiance_2023']
print(nsrdb_file)
    
with NSRDBX(nsrdb_file, hsds=True) as f:
    meta = f.meta
    time_index = f.time_index
    dni = f['dni']
    
print(nsrdb_file.shape())
    
def convert_h5_to_parquet(h5_file_path, parquet_file_path):
    with h5py.File(h5_file_path, "r") as f:
        df = pd.DataFrame(f["data"])
    df.to_parquet(parquet_file_path)
    
