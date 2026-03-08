import pandas as pd
from ingest import ingest_data

def clean_data():
    df = ingest_data()
    new_cols = ["order_id", "quantity", "product", "unit_price", "order_date", "customer_name", "region", "total_amount"]
    required_items = ["ORDERNUMBER", "QUANTITYORDERED", "PRODUCTLINE", "PRICEEACH", "ORDERDATE", "CUSTOMERNAME", "COUNTRY", "SALES"]
    filtered_df = df.filter(items=required_items)
    filtered_df.columns = new_cols
    print(filtered_df.head(3))


if __name__ == "__main__":
    clean_data()