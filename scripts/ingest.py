import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ingest_data():
    df = pd.read_csv("data/sales_data_sample.csv", encoding='latin1')
    logging.info(f"Ingested {len(df)} rows")
    return df