/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
 This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
--Collect all cust info from the two source systems
--After joining, check if duplicates were introduced -> No dups
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
create view gold.dim_customers as
		select 
	--Given two gender cols, sort 
	--remane columns with friendly names
	--Order how you want cols to appear as
	--Check table: is it a dimension or fact
	--Gen surrogate key to use on dim table
		ROW_NUMBER() over (order by ci.cust_id) as customer_key,
		ci.cust_id as customer_id, 
		ci.cust_key as customer_number,
		ci.cust_firstname as first_name,
		ci.cust_lastname as last_name,
		loc.country as country,
		ci.cust_marital_status as marital_status,
		case when ci.cust_gender != 'n/a' then ci.cust_gender
			else coalesce(ca.gender, 'n/a') -- where null - n/a, filling gender with erp gender when n/a in crm
		end as gender,
		ca.birthdate as birth_date,
		ci.cust_create_date as create_date		
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cust_key  = ca.cust_id
	left join silver.erp_loc_a101 loc
	on ci.cust_key  = loc.cid


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
--Current data for products 
--Reorder to organize
--Rename
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
create view gold.dim_products as
select 
	ROW_NUMBER () over (order by prod_start_date, prod_key) as product_key,
	prod.prod_id as product_id,
	prod.prod_key as product_number,
	prod.prod_name as product_name,
	prod.cat_id as category_id,
	pc.Category as category,
	pc.Subcategory as subcategory,
	pc.Maintenance as maintenance,
	prod.prod_cost as cost ,
	prod.prod_line as product_line,
	prod.prod_start_date as start_date
from silver.crm_prod_info prod
left join silver.erp_px_cat_g1v2 pc
on prod.cat_id = pc.id
where prod_end_date is null -- filter historical data 

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
--Use dim surrogate keys instead of ids to connect facts to dims
--Data Lookup?
--Rename
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
create view gold.fact_sales as
select 
	sales_order_num as order_number,
	pr.product_key,
	cust.customer_key,
	sales_order_dt as order_date,
	sales_ship_dt as shipping_date,
	sales_due_dt as due_date,
	sls_sales as sales_amount,
	sales_qty as quantity,
	sales_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr 
on sd.sales_prod_key = pr.product_number
left join gold.dim_customers cust
on sd.sales_cust_id = cust.customer_id
