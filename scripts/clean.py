import pandas as pd
from sqlalchemy import true

from ingest import ingest_data
import json
import logging

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
    total_null_count = df.isnull().sum()
    total_negative_count = (df < 0).sum().sum()
    null_customer_count = df["customer_name"].isnull().sum()
    duplicate_customer_name_count = df["customer_name"].duplicated().sum()
    null_order_id_count = df["order_id"].isnull().sum()
    duplicate_order_id_count = df['order_id'].duplicated().sum()
    negative_quantity_count = df(df['quantity'] < 0 ).sum()
    negative_total_amount_count = df(df['total_amount'] < 0 ).sum()

    tracking_invalid_data = {
        "row_count": row_count,
        "total_null_count": total_null_count,
        "total_negative_count": total_negative_count,
        "null_customer_count": null_customer_count,
        "duplicate_customer_name": duplicate_customer_name_count,
        "null_order_id_count": null_order_id_count,
        "duplicate_order_id_count": duplicate_order_id_count,
        "negative_quantity_count": negative_quantity_count,
        "negative_total_amount_count": negative_total_amount_count
    }
    file_path = 'tracking_invalid_data.json'

    with open(file_path, 'w') as json_file:
        json.dump(tracking_invalid_data, json_file, indent=4)

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
    df = ingest_data()
    modified_df = rename_columns(df)
    track_invalid_records(modified_df)
    cleaned_df = remove_invalid_records(modified_df)
    logging.info("Data has been cleaned!")
    return cleaned_df


if __name__ == "__main__":
    clean_data()