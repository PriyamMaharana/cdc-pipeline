-- File: cdc_setup.sql
-- using SCD_Project Database
USE SCD_Project;
GO

-- creating the source table 
-- this will be the table monitored by CDC
CREATE TABLE orders (
	order_id INT PRIMARY KEY IDENTITY(1,1),
	customer_id INT NOT NULL,
	product_name NVARCHAR(100) NOT NULL,
	quantity INT NOT NULL,
	unit_price DECIMAL(10,2) NOT NULL,
	total_amount AS (quantity * unit_price) PERSISTED,
	order_status NVARCHAR(20) DEFAULT 'Pending',
	order_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
	created_at DATETIME2 DEFAULT GETDATE(),
	updated_at DATETIME2 DEFAULT GETDATE()
);
GO

-- enable CDC on database first
-- (only one time per databse)
EXEC sys.sp_cdc_enable_db;
GO

-- verifying database CDC is enabled or not
SELECT name, is_cdc_enabled
from sys.databases
where name = 'SCD_Project';
GO

-- Step 1: Drop the existing capture instance
EXEC sys.sp_cdc_disable_table
    @source_schema   = N'dbo',
    @source_name     = N'orders',
    @capture_instance = N'dbo_orders';
GO

-- enable cdc on the orders table
EXEC sys.sp_cdc_enable_table
	@source_schema = N'dbo',
	@source_name = N'orders',
	@role_name = NULL,
	@supports_net_changes = 1;
GO

-- verify tbale CDc is enabled
SELECT s.name as schema_name, t.name as table_name, t.is_tracked_by_cdc
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name = 'orders';
GO

-- check what CDC created automatically
-- sql server created hidden system tables to track changes
SELECT name
FROM sys.tables
WHERE SCHEMA_NAME(schema_id) LIKE 'cdc%';
GO


-- create target/destination table
-- this is where our pipeline loads data into (eg. data warehouse orders table)
CREATE TABLE orders_warehouse (
	warehouse_id INT PRIMARY KEY IDENTITY(1,1),
	order_id INT NOT NULL,
	customer_id INT NOT NULL,
	product_name NVARCHAR(100),
	quantity INT,
	unit_price DECIMAL(10,2),
	total_amount DECIMAL(10,2),
	order_status NVARCHAR(20) DEFAULT 'Pending',
	order_date DATE,
	cdc_operation NVARCHAR(10), -- INSERT/UPDATE/DELETE
	loaded_at DATETIME2 DEFAULT GETDATE()
);
GO

-- pipeline tracking table, this store last LSN (log seq. no) processed
-- (eg. bookmark - "processed upto this point")
CREATE TABLE cdc_watermark (
	id INT PRIMARY KEY IDENTITY(1,1),
	table_name NVARCHAR(100) NOT NULL,
	last_lsn BINARY(10),
	last_run DATETIME2 DEFAULT GETDATE(),
	rows_processed INT DEFAULT 0,
	status NVARCHAR(20)  -- SUCCESS/FAILED
);
GO

-- error logs table
CREATE TABLE cdc_error_log (
	error_id INT PRIMARY KEY IDENTITY(1,1),
	error_name DATETIME2 DEFAULT GETDATE(),
	table_name NVARCHAR(100),
	error_msg NVARCHAR(MAX),
	lsn_from BINARY(10),
	lsn_to BINARY(10)
);
GO


select * from Orders;
SELECT TOP 20 order_id FROM orders ORDER BY NEWID();
select * from cdc_watermark;

