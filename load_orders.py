import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
from datetime import date, datetime, timedelta
import logging
import os

## Setup Logging
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO, # Changed to INFO for cleaner console, DEBUG is very noisy for SQLAlchemy
    format='%(asctime)s | %(levelname)s | %(message)s', # Fixed typo: acstime -> asctime
    handlers=[
        logging.FileHandler('logs/orders_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

## Database Connection
# Define the connection string separately
connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=SCD_Project;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Use urllib to quote the string for SQLAlchemy
params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

## Load Data
def load_orders(file_path, table_name, row_limit=50):
    try:
        # Check if file exists first
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return

        df = pd.read_csv(file_path, nrows=row_limit)
        logger.info(f"Loading {len(df)} records into {table_name}...")
        
        # Use the engine directly
        df.to_sql(table_name, schema='dbo', con=engine, if_exists='append', index=False)
        logger.info("Data loaded successfully!!")
        
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure the directory exists or provide the full path
    load_orders('data/initial_orders.csv', 'orders', row_limit=50)