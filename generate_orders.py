import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

PRODUCTS = ['Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Webcam', 'USB Hub', 'Desk Lamp',
            'Table', 'Chair', 'Standing Desk', 'Mobile', 'Router']

PRODUCT_PRICE = {
    "Laptop": 45000, "Monitor": 14500, "Keyboard": 895, "Mouse": 499, "Headphones": 2599,
    "Webcam": 999, "USB Hub": 700, "Desk Lamp": 1100, "Table": 5999, "Chair": 1800,
    "Standing Desk": 4050, "Mobile": 21000, "Router": 800
}

STATUS = ['Pending', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled']
STATUS_WGT = [0.10, 0.15, 0.20, 0.50, 0.05]

def initial_order(n=10000):
    records = []
    str_date = datetime.now()
    
    for _ in range(n):
        product = random.choice(PRODUCTS)
        order_status = random.choices(STATUS, weights=STATUS_WGT, k=1)[0]
        records.append({
            'customer_id': random.randint(1, 1000),
            'product_name': product,
            'quantity': random.randint(1, 10),
            'unit_price': PRODUCT_PRICE[product],
            'order_status': order_status,
            'order_date': (str_date - timedelta(days=random.randint(0,7))).date()
        })
        
    df = pd.DataFrame(records)
    df.to_csv('data/initial_orders.csv', index=False)
    print(f"Generated Orders - {len(df)} records.")
    return df

if __name__ == "__main__":
    import os
    os.makedirs('data', exist_ok=True)
    
    initial = initial_order(10000)
    print(f"The data is generated.")