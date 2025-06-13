#!/bin/bash
# This script runs the entire ECMWF data retrieval pipeline as jobs

# Parameters
START_YEAR=2016
END_YEAR=2024
INIT_TIMES=("0000" "1200")
LEAD_TIMES=("0" "6" "12" "18" "24" "48" "72" "96" "120" "168" "240")

# Retrieve forecast data
prev_job_id=""
for year in {$START_YEAR..$END_YEAR}; do
    if [ -z "$prev_job_id" ]; then
        job_id=$(qsub -v YEAR=$year get_ecmwf_fc.pbs)
    else
        job_id=$(qsub -W depend=afterok:$prev_job_id -v YEAR=$year get_ecmwf_fc.pbs)
    fi
    prev_job_id=$job_id
    echo "Submitted forecast retrieval job for year $year with job ID: $job_id"
done

# Retrieve analysis data
for year in {$START_YEAR..$END_YEAR}; do
    job_id=$(qsub -W depend=afterok:$prev_job_id -v YEAR=$year get_ecmwf_an.pbs)
    prev_job_id=$job_id
    echo "Submitted analysis retrieval job for year $year with job ID: $job_id"
done

# Sort files by year and month
job_id=$(qsub -W depend=afterok:$prev_job_id sort_ecmwf.pbs)
prev_job_id=$job_id
echo "Submitted sorting job with job ID: $job_id"

# Convert forecast data from grib to netCDF
for init_time in "${INIT_TIMES[@]}"; do
    for lead_time in "${LEAD_TIMES[@]}"; do
        job_id=$(qsub -W depend=afterok:$prev_job_id -v INIT_TIME=$init_time,LEAD_TIME=$lead_time convert_ecmwf_fc.pbs)
        echo "Submitted forecast conversion job for init time $init_time and lead time $lead_time with job ID: $job_id"
    done
done

# Submit analysis data conversion job
job_id=$(qsub -W depend=afterok:$prev_job_id convert_ecmwf_an.pbs)
echo "Submitted analysis conversion job with job ID: $job_id"

# Final message
echo "All jobs submitted. Please monitor the job queue for progress."