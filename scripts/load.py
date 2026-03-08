from sqlalchemy import create_engine
from transform import transform

engine = create_engine('mysql+pymysql://root:root@localhost:3306/sales_data_01')

def load_data():
    daily_sales, top_products = transform
    daily_sales.to_sql("daily_sales", engine, if_exists="replace", index=False)
    top_products.to_sql("top_products", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    load_data()