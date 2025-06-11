#!/bin/bash
# This script organizes ECMWF files by year and month based on their filenames.

## Forecast
BASE_DIR="/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc/0.125/2t"
INIT_TIMES=("0000" "1200")
LEAD_TIMES=("0" "6" "12" "18" "24" "48" "72" "96" "120" "168" "240")
SOURCE_DIRS=()

for init_time in "${INIT_TIMES[@]}"; do
    for lead_time in "${LEAD_TIMES[@]}"; do
        SOURCE_DIRS+=("$BASE_DIR/$init_time/$lead_time/")
    done
done

for SOURCE_DIR in "${SOURCE_DIRS[@]}"; do
    cd "$SOURCE_DIR" || { echo "Failed to access $SOURCE_DIR"; continue; }
    echo "Organizing files in $SOURCE_DIR..."
    for file in ifs_fc_2t_*.grib; do
        year=$(echo "$file" | cut -d'_' -f6 | cut -c1-4)
        month=$(echo "$file" | cut -d'_' -f6 | cut -c5-6)
        mkdir -p "$year/$month"
        mv "$file" "$year/$month/"
    done
    echo "Files in $SOURCE_DIR have been organized by year and month."
done

## Analysis
BASE_DIR="/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an/0.125/2t"

cd "$BASE_DIR" || exit
echo "Organizing files in $BASE_DIR..."
for file in ifs_an_2t_*.grib; do
    year=$(echo "$file" | cut -d'_' -f4 | cut -c1-4)
    month=$(echo "$file" | cut -d'_' -f4 | cut -c5-6)
    mkdir -p "$year/$month"
    mv "$file" "$year/$month/"
done

echo "Files in $BASE_DIR have been organized by year and month."