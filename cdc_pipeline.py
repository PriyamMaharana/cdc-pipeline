import pandas as pd
import logging
import os
from datetime import date, datetime, timedelta
import pyodbc

## 1. Logging Setup
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/cdc_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

## 2. Database Connection
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for Sql Server};"
        "SERVER=localhost;"
        "DATABASE=SCD_Project;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    
## 3. Getting LSN (log sequence num.) range
## [every change in sql server gets a unique LSN]
def get_lsn_range(conn, table_name):
    """
    Get the LSN range to process:
        - lsn_from: where we left off last run (stored in cdc_watermark table)
        - lsn_to: the latest LSN available right now
    """
    cursor = conn.cursor()
    
    # get last processed LSN from watermark
    cursor.execute("SELECT last_lsn FROM cdc_watermark WHERE table_name = ?", table_name)
    row = cursor.fetchone()
    
    # get min LSN available in CDC log
    cursor.execute("SELECT sys.fn_cdc_get_min_lsn('dbo_orders')")
    min_lsn = cursor.fetchone()[0]
    
    # get max LSN available in CDC log
    cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
    max_lsn = cursor.fetchone()[0]
    
    if row is None or row[0] is None:
        lsn_from = min_lsn
        logger.info(f"First run - starting from minimum LSN")
    else:
        lsn_from = row[0]
        logger.info(f"Resuming from last processed LSN")
    
    logger.info(f"LSN range: {lsn_from.hex()} -> {max_lsn.hex()}")
    return lsn_from, max_lsn


## 4. Read CDC Changes
## Sql server CDC function return all changes between two LSN values
## __$operation Codes: 1 - Delete, 2 - Insert, 3 - Update(before img: old value), 4 - Update(after img: new value)
def read_cdc_changes(conn, lsn_from, lsn_to):
    query = """
        SELECT __$operation, __$start_lsn, order_id, customer_id, product_name,
        quantity, unit_price, total_amount, order_status, order_date
        FROM cdc.fn_cdc_get_all_changes_dbo_orders (?,?,'all') ORDER BY __$start_lsn, __$operation;
    """
    
    df = pd.read_sql(query, conn, params=[lsn_from, lsn_to])
    
    if df.empty:
        logger.info(f"No changes detected since last run")
        return df
    
    ## Map operation code to readable names
    operation_map = {
        1: 'DELETE',
        2: 'INSERT',
        3: 'UPDATE_BEFORE',
        4: 'UPDATE_AFTER'
    }
    df['operation_map'] = df['__$operation'].map(operation_map)
    
    logger.info(f"CDC changes found: {len(df)} raw records.")
    logger.info(
        f"Breakdown: "
        f"INSERT={len(df[df['__$operation']==2])} |"
        f"UPDATE={len(df[df['__$operation']==4])} |"
        f"DELETE={len(df[df['__$operation']==1])}"
    )   
    return df

## 5. Process and load chnages
def process_changes(conn, changes_df):
    if changes_df.empty:
        return 0
    
    cursor = conn.cursor()
    rows_processed = 0
    
    for _, row in changes_df.iterrows():
        operation = row['__$operation']
        
        ### Insert - new order arrived
        if 2 == operation:
            query = """
                INSERT INTO orders_warehouse(order_id, customer_id, product_name,
                quantity, unit_price, total_amount, order_status, order_date, cdc_operation) 
                VALUES (?,?,?,?,?,?,?,?,'INSERT');
            """
            
            cursor.execute(
                query,
                int(row['order_id']),
                int(row['customer_id']),
                str(row['product_name']),
                int(row['quantity']),
                float(row['unit_price']),
                float(row['total_amount']) if row['total_amount'] else 0,
                str(row['order_status']), 
                str(row['order_date'])
            )
            
            rows_processed +=1
            
        ### Update after - order was modified
        elif 4 == operation:
            query = """
                UPDATE orders_warehouse SET order_status = ?, quantity = ?,
                unit_price = ?, cdc_operation = 'UPDATE_AFTER', loaded_at = GETDATE() 
                WHERE order_id = ?;
            """
            
            cursor.execute(
                query,
                str(row['order_status']),
                int(row['quantity']),
                float(row['unit_price']),
                int(row['order_id'])
            )
            
            # If no row was updated, insert it (handles case where 
            # order wasn't in warehouse from a previous run)
            if 0 == cursor.rowcount:
                query = """
                    INSERT INTO orders_warehouse(order_id, customer_id,
                    product_name, quantity, unit_price, total_amount, order_status,
                    order_date, cdc_operation) VALUES(?,?,?,?,?,?,?,?,'UPDATE_AFTER');
                """
                
                cursor.execute(
                    query,
                    int(row['order_id']),
                    int(row['customer_id']),
                    str(row['product_name']),
                    int(row['quantity']),
                    float(row['unit_price']),
                    float(row['total_amount']) if row['total_amount'] else 0,
                    str(row['order_status']), 
                    str(row['order_date'])
                )
                
                rows_processed +=1

        ### Delete - order was removed
        elif 1 == operation:
            query = """
                UPDATE orders_warehouse SET cdc_operation = 'DELETE', 
                loaded_at = GETDATE() WHERE order_id = ?;
            """
            
            cursor.execute(
                query, int(row['order_id'])
            )
            
            rows_processed +=1
    
    conn.commit()
    logger.info(f"Loaded {rows_processed} changes to orders_warehouse")
    return rows_processed

## 6. Update Watermark
## saves LSN we processed upto next run from here
def update_watermark(conn, table_name, lsn_to, rows_processed, status):
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM cdc_watermark WHERE table_name = ?", table_name)
    row = cursor.fetchone()
    
    if row:
        cursor.execute(
            """ UPDATE cdc_watermark SET last_lsn = ?, last_run = GETDATE(),
            rows_processed = ?, status = ? WHERE table_name = ?; """,
            lsn_to, rows_processed, status, table_name
        )
    else:
        cursor.execute(
            """ INSERT INTO cdc_watermark(table_name, last_lsn, 
            rows_processed, status) VALUES (?,?,?,?); """,
            table_name, lsn_to, rows_processed, status
        )
        
    conn.commit()
    logger.info(f"Watermark updated -> LSN: {lsn_to.hex()}")
    

## Main pipeline Orchestrator
def run_cdc_pipeline():
    logger.info("="*60)
    logger.info(f"CDC Pipeline Started — {datetime.now()}")
    logger.info("Table: dbo.orders → orders_warehouse")
    logger.info("="*60)
    
    TABLE_NAME = 'dbo_orders'
    conn = get_connection()
    
    try:
        # get lsn range
        lsn_from, lsn_to = get_lsn_range(conn, TABLE_NAME)
        
        # check if there anything new to process
        if lsn_from == lsn_to:
            logger.info(f"No new chnages since last run. Exiting..!!")
            update_watermark(conn, TABLE_NAME, lsn_to, 0, 'SUCCESS')
            return
        
        # read CDC changes
        changes_df = read_cdc_changes(conn, lsn_from, lsn_to)
        
        # process and load chnages
        rows_processed = process_changes(conn, changes_df)
        
        # update watermark
        update_watermark(
            conn, TABLE_NAME, lsn_to, rows_processed, 'SUCCESS'
        ) 
        
        logger.info(f"Pipeline Completed - {rows_processed} rows processed.")
        logger.info("="*60)
        
    except Exception as e:
        logger.info(f"Pipeline FAILED: {str(e)}")
        
        try:
            update_watermark(
                conn, TABLE_NAME, lsn_to, 0, 'FAILED'
            )
        except:
            pass
        raise
    finally:
        conn.close()
        
        
if __name__ == "__main__":
    run_cdc_pipeline()
