import pandas as pd
from src.clean import remove_invalid_records, track_invalid_records

def test_track_invalid_records():
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
    result = track_invalid_records(df)

    assert result["row_count"] == len(df)

def test_remove_invalid_records():
    data = {
        "order_id": [1, None],
        "quantity": [2, None],
        "product": ["Samsung", None],
        "unit_price": [100, None],
        "order_date": ["2023/03/04", None],
        "customer_name": ["karthick", None],
        "region": ["india", None],
        "total_amount": [100, None]
    }
    df = pd.DataFrame(data)
    result = remove_invalid_records(df)

    assert result["customer_name"].tolist() == [df.loc[0, "customer_name"]]
