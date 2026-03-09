from config.logging_config import setup_logging
from scripts import load

setup_logging()

def call_etl_process():
    load.load_data()

if __name__ == "__main__":
    call_etl_process()