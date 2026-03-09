import pandas as pd
from sqlalchemy import true
from src import ingest
import json
import logging

logger = logging.getLogger(__name__)

def rename_columns(df):
    new_cols = ["order_id", "quantity", "product", "unit_price", "order_date", "customer_name", "region",
                "total_amount"]
    required_items = ["ORDERNUMBER", "QUANTITYORDERED", "PRODUCTLINE", "PRICEEACH", "ORDERDATE", "CUSTOMERNAME",
                      "COUNTRY", "SALES"]
    filtered_df = df.filter(items=required_items)
    filtered_df.columns = new_cols

    logging.info("Required columns have been renamed")
    return filtered_df

def track_invalid_records(df):
    row_count = len(df)
    total_null_count = df.isnull().sum().sum()
    null_customer_count = df["customer_name"].isnull().sum()
    duplicate_customer_name_count = df["customer_name"].duplicated().sum()
    null_order_id_count = df["order_id"].isnull().sum()
    duplicate_order_id_count = df['order_id'].duplicated().sum()
    negative_quantity_count = df[df["quantity"] < 0].sum().sum()
    negative_total_amount_count = df[df['total_amount'] < 0].sum().sum()

    tracking_invalid_data = {
        "row_count": int(row_count),
        "total_null_count": int(total_null_count),
        "null_customer_count": int(null_customer_count),
        "duplicate_customer_name_count": int(duplicate_customer_name_count),
        "null_order_id_count": int(null_order_id_count),
        "duplicate_order_id_count": int(duplicate_order_id_count),
        "negative_quantity_count": int(negative_quantity_count),
        "negative_total_amount_count": int(negative_total_amount_count)
    }

    return tracking_invalid_data

def write_track_invalid_records_into_file(data):
    file_path = 'tracking_invalid_data.json'
    data.update({"file_name": "data/sales_data_sample.csv"})

    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    logging.info("Invalid records have been tracked!")
    return true()

def remove_invalid_records(df):
    df.drop_duplicates(inplace=True)
    df = df.dropna(subset=["customer_name"])
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df[df["quantity"] > 0]
    logging.info("Invalid records have been removed!")
    return df

def clean_data():
    df = ingest.ingest_data()
    modified_df = rename_columns(df)
    data = track_invalid_records(modified_df)
    write_track_invalid_records_into_file(data)
    cleaned_df = remove_invalid_records(modified_df)
    logging.info("Data has been cleaned!")
    return cleaned_df