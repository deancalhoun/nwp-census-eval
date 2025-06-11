#!/bin/bash
# This script converts ECMWF files from grib format to netCDF format

module load eccodes

BASE_DIR="/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an/0.125/2t"

for file in ${BASE_DIR}/*/*/*.grib; do
    grib_to_netcdf -o "${file%.*}.nc" "$file" 
    rm "$file"
done