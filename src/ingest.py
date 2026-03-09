import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def ingest_data():
    cwd = os.getcwd()
    filename = os.path.join(cwd, "data", "sales_data_sample.csv")
    # filename = "/home/karthick/PycharmProjects/sales_data_pipeline/data/sales_data_sample.csv"
    df = pd.read_csv(filename, encoding='latin1')
    logging.info(f"Ingested {len(df)} rows")
    return df