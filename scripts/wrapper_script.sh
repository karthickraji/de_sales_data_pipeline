#!/bin/bash

echo "Starting Sales ETL Pipeline"

# go to project root
# shellcheck disable=SC2164
cd /home/karthick/PycharmProjects/sales_data_pipeline

# activate virtual environment (optional)
source venv/bin/activate

# run pipeline
python main.py

echo "Pipeline Finished"