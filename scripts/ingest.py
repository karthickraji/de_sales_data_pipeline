import pandas as pd
import logging

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO
)

def ingest_data():
    df = pd.read_csv("data/sales_data_sample.csv", encoding='latin1')
    logging.info(f"Ingested {len(df)} rows")
    return df

if __name__ == '__main__':
    ingest_data()