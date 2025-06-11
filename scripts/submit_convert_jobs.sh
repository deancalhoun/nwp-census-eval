#!/bin/bash

# Initialize previous job ID
prev_job_id=""

# Submit jobs
for init_time in {0000,1200}; do
    for lead_time in {0,6,12,18,24,48,72,96,120,168,240}; do
        # Submit the job with the current init_time and lead_time
        job_id=$(qsub -v INIT_TIME=$init_time,LEAD_TIME=$lead_time convert_ecmwf_fc.pbs)
        echo "Submitted conversion job for init time $init_time and lead time $lead_time with job ID: $job_id"
    done
done