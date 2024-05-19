#!/bin/bash

# Create the bash_run_log folder if it doesn't exist
mkdir -p bash_run_logs

# Run iostat in a loop to continuously monitor I/O and append output to the log file
while true; do
    iostat -x 1 >> bash_run_logs/iostat_output.txt
done &

# Start top in batch mode and redirect output to a file
top -b -d 1 > bash_run_logs/top_output.txt &

# Wait for the Python script to finish execution
python scripts/03_generate_1_day_sales_data.py "$@"

# Kill iostat and top processes after the Python script finishes
killall iostat
killall top