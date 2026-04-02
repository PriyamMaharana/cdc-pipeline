use SCD_Project;
go

-- whats in warehouse
select 
	order_id, product_name, quantity, order_status, cdc_operation, loaded_at
from orders_warehouse 
order by loaded_at desc;

-- breakdown by cdc
select 
	cdc_operation, count(*) as count
from orders_warehouse
group by cdc_operation;

-- checking watermark (like what lsn we reach)
select
	table_name, last_lsn, last_run, rows_processed
from cdc_watermark
order by last_run desc;

-- compare source vs warehouse
select 'orders (source)' as tbl, count(*) as rows from orders
union all
select 'order_warehouse', count(*) from orders_warehouse
union all
select 'cdc change table', count(*) from cdc.dbo_orders_CT;

-- view what sql server capture automatically
select top 20
	__$operation, __$start_lsn, order_id, order_status, __$update_mask
from cdc.dbo_orders_CT
order by __$start_lsn desc;