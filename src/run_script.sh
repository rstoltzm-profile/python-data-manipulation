#!/bin/bash
echo "Running data clean script"
python3 1-clean_process_population_data.py
echo "Running data transform script"
python3 2-transform_process_population_data.py 