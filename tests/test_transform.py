import pandas as pd
from src.transform import daily_sales, top_products

def test_daily_sales():
    data = {
        "order_id": [1],
        "quantity": [2],
        "product": ["Samsung"],
        "unit_price": [100],
        "order_date": ["2023/03/04"],
        "customer_name": ["karthick"],
        "region": ["india"],
        "total_amount": [100]
    }
    df = pd.DataFrame(data)
    df["order_date"] = pd.to_datetime(df["order_date"])
    result = daily_sales(df)
    data = df.groupby('order_date').agg(total_revenue=('unit_price', 'sum')).reset_index()

    assert result["total_revenue"].tolist() == data["total_revenue"].tolist()

def test_top_products():
    data = {
        "order_id": [1],
        "quantity": [2],
        "product": ["Samsung"],
        "unit_price": [100],
        "order_date": ["2023/03/04"],
        "customer_name": ["karthick"],
        "region": ["india"],
        "total_amount": [100]
    }
    df = pd.DataFrame(data)
    result = top_products(df)

    data = df.groupby('product').agg(total_qty=('quantity', 'sum')).sort_values('total_qty', ascending=False)

    assert result["total_qty"].tolist() == data["total_qty"].tolist()