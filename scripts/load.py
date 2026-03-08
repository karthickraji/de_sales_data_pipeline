from sqlalchemy import create_engine
from transform import transform
import logging
from config.db_config import *

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_data():
    daily_sales, top_products = transform
    daily_sales.to_sql("daily_sales", engine, if_exists="replace", index=False)
    top_products.to_sql("top_products", engine, if_exists="replace", index=False)
    logging.info("Data loaded successfully")
    print("Data loaded successfully")

if __name__ == "__main__":
    load_data()