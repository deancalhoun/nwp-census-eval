#!/bin/bash
# This script converts ECMWF files from grib format to netCDF format

module load eccodes

## Forecast
BASE_DIR="/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc/0.125/2t"

# Read INIT_TIME and LEAD_TIME from command-line arguments
INIT_TIME=$1
LEAD_TIME=$2

if [[ -z "$INIT_TIME" || -z "$LEAD_TIME" ]]; then
    echo "Usage: $0 <INIT_TIME> <LEAD_TIME>"
    exit 1
fi

for file in ${BASE_DIR}/${INIT_TIME}/${LEAD_TIME}/*/*/*.grib; do
    grib_to_netcdf -o "${file%.*}.nc" "$file" 
    rm "$file"
done