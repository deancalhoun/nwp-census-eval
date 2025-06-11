#!/bin/bash
#!/bin/bash

# Initialize previous job ID
prev_job_id=""

# Submit jobs
for year in {2020..2024}; do
    job_id=$(qsub -v YEAR=$year aggregate.pbs)
    echo "Submitted analysis aggregation job for year $year with job ID: $job_id"
done