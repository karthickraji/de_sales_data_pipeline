from clean import clean_data
import logging

def daily_sales(df):
    return (
        df.groupby('order_date').agg(total_revenue=('order_date', 'sum')).reset_index()
    )

def top_products(df):
    return (
        df.groupby('product').agg(total_qty=('quantity', 'sum')).sort_values('total_qty', ascending=False).reset_index()
    )

def transform():
    df = clean_data()
    daily_sales_agg = daily_sales(df)
    top_products_agg = top_products(df)

    logging.info("Transformation completed")
    return daily_sales_agg, top_products_agg

if __name__ == "__main__":
    transform()