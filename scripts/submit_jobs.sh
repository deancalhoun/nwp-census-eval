#!/bin/bash

# Initialize previous job ID
prev_job_id=""

# Submit jobs
for year in {2016..2024}; do
    if [ -z "$prev_job_id" ]; then
        # Submit the first job without dependency
        job_id=$(qsub -v YEAR=$year get_ecmwf_analysis.pbs)
    else
        # Submit subsequent jobs with dependency
        job_id=$(qsub -W depend=afterok:$prev_job_id -v YEAR=$year get_ecmwf_analysis.pbs)
    fi

    prev_job_id=$job_id
    echo "Submitted analysis retrieval job for year $year with job ID: $job_id"
done