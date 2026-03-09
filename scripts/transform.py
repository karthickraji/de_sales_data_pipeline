from scripts import clean
import logging

logger = logging.getLogger(__name__)

def daily_sales(df):
    return (
        df.groupby('order_date').agg(total_revenue=('unit_price', 'sum')).reset_index()
    )

def top_products(df):
    return (
        df.groupby('product').agg(total_qty=('quantity', 'sum')).sort_values('total_qty', ascending=False).head(10)
    )

def transform_data():
    df = clean.clean_data()
    daily_sales_agg = daily_sales(df)
    top_products_agg = top_products(df)

    logging.info("Transformation completed")
    return daily_sales_agg, top_products_agg