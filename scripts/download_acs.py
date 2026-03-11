import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval.data import download_acs

# Parameters
acs_dir = '/glade/derecho/scratch/dcalhoun/census/data/acs_5yr_2023'
acs_tables = [
    'B01001', 'B01002', 'B02001', 'B03001', 'B15002', 'B15003',
    'B17001', 'B19001', 'B19013', 'B23025', 'B25001', 'B25002',
    'B25003', 'B25064', 'B25077'
]
year = 2023
level = 'county'  # or 'tract'

# Download and save
df = download_acs(year=year, groups=acs_tables, level=level, estimate_only=True)
os.makedirs(acs_dir, exist_ok=True)
out_path = os.path.join(acs_dir, f'acs_5yr_{year}_{level}.parquet')
df.to_parquet(out_path, index=False)
print(f"Saved {len(df)} rows to {out_path}")
print(df.head())
