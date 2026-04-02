import pandas as pd
import pyodbc
import logging
import random
import os
from datetime import date, timedelta

random.seed(42)

PRODUCTS = ['Laptop', 'Monitor', 'Mobile', 'Keyboard', 'Table', 'Headphones', 
            'Webcam', 'USB Hub', 'Desk Lamp', 'Mouse', 'Chair', 
            'Standing Desk', 'Router']

PRODUCT_PRICE = {
    "Laptop": 45000, "Monitor": 14500, "Mobile": 21000, "Keyboard": 895,
    "Table": 5999, "Headphones": 2599, "Webcam": 999, "USB Hub": 700, 
    "Desk Lamp": 1100, "Mouse": 499, "Chair": 1800, "Standing Desk": 4050, "Router": 800
}

STATUS = ['Pending', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled']
STATUS_WGT = [0.10, 0.15, 0.20, 0.50, 0.05]

## SetUp Logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/order_transaction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

## Database Connection
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for Sql Server};"
        "SERVER=localhost;"
        "DATABASE=SCD_Project;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    
## Order Transaction
### 1st round of transaction (insert new records)
def round1_insert_records():
    # print(f"Round 1: Inserting first 30 new records..")
    logger.info(f"Round 1: Inserting first 30 new records..")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    for _ in range(60):
        customer_id = random.randint(50, 150)
        product = random.choice(PRODUCTS)
        price = PRODUCT_PRICE[product]
        qty = random.randint(1,5)
        order_sts = random.choice(STATUS[:3])
        order_dt = date(2026, 4, random.randint(1,5))
        
        query = """
            INSERT INTO orders (customer_id, product_name, quantity, unit_price,
            order_status, order_date) VALUES (?,?,?,?,?,?);
        """
        
        cursor.execute(
            query, customer_id, product, qty, price, order_sts, str(order_dt)
        )
        
    conn.commit()
    conn.close()
    # print(f"Round 1 complete - 60 orders inserted\n")
    logger.info(f"Round 1 complete - 60 orders inserted\n")
    
    
### 2nd round of transaction (update status)
def round2_update_status():
    # print(f"Round 2: Update order status..")
    logger.info(f"Round 2: Update order status..")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT TOP 20 order_id FROM orders ORDER BY NEWID();")
    order_ids = [row[0] for row in cursor.fetchall()]
    
    for order_id in order_ids:
        new_status = random.choice(STATUS[1:4])
        query = """
            UPDATE orders SET order_status = ?, updated_at = GETDATE() 
            WHERE order_id = ?;
        """
        
        cursor.execute(
            query, new_status, order_id
        )
        
    conn.commit()
    conn.close()
    # print(f"Round 2 completed - {len(order_ids)} orders updated.\n")
    logger.info(f"Round 2 completed - {len(order_ids)} orders updated.\n")

### 3rd round transaction (cancellation and new records)
def round3_cancellation_and_new():
    print(f"Round 3: Cancellations +  New Orders...")
    logger.info(f"Round 3: Cancellations +  New Orders...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    #### cancell 5 orders
    cursor.execute("SELECT TOP 5 order_id FROM orders ORDER BY NEWID();")
    cancel_ids = [row[0] for row in cursor.fetchall()]
    
    for order_id in cancel_ids:
        new_status = STATUS[-1]
        query = """
            UPDATE orders SET order_status = ?, updated_at = GETDATE() 
            WHERE order_id = ?;
        """
        
        cursor.execute(
            query, new_status, order_id
        )
        
    #### Add 5 new high value
    for _ in range(5):
        product = random.choice(PRODUCTS[:6])
        price = PRODUCT_PRICE[product]
        customer_id = random.randint(50, 150)
        qty = random.randint(1,5)
        order_dt = date(2026, 4, random.randint(5,9))
        
        query = """
            INSERT INTO orders (customer_id, product_name, quantity, unit_price,
            order_status, order_date) VALUES (?,?,?,?,'Pending',?);
        """
        
        cursor.execute(
            query, customer_id, product, qty, price, str(order_dt)
        )
    
    conn.commit()
    conn.close()
    # print(f"Round 3 complete - {len(cancel_ids)} cancelled, 5 new orders.\n")
    logger.info(f"Round 3 complete - {len(cancel_ids)} cancelled, 5 new orders.\n")
       

if __name__ == "__main__":
    print("="*40)
    print("Transaction Generator - CDC ")
    print("="*40)
    print("\nWhich round want to run??")
    print("1 - New orders \n2 - Status Update \n3 - Cancellations + New Orders \nall - Runs all 3 in sequence") 
    
    choice = input("\nEnter Choice: ").strip().lower()
    
    if '1' == choice:
        round1_insert_records()
    elif '2' == choice:
        round2_update_status()
    elif '3' == choice:
        round3_cancellation_and_new()
    elif 'all' == choice:
        round1_insert_records()
        round2_update_status()
        round3_cancellation_and_new()
    else:
        print("\nInvalid Choice")