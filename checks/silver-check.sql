/*
===============================================================================
Quality Checks
===============================================================================
 This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

----------------------Customer Table---------------------------------
---Check Duplicates or nulls in pk
SELECT cust_id,
COUNT(*)
  FROM silver.crm_cust_info 
GROUP BY cust_id
HAVING COUNT(*)>1 OR cust_id IS NULL

---Check unwanted spaces
select cust_firstname
from silver.crm_cust_info
--if original values is not equal to value after trimming, there are spaces
where cust_firstname != TRIM(cust_firstname) 

--Data Standardization & Consistency
SELECT DISTINCT	cust_gender
from silver.crm_cust_info

SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;


--------------------Product Table------------------------------------
SELECT * FROM bronze.crm_prod_info
where prod_name = 'HL Road Frame - Red- 58'


--Checking null or duplicates 
select
	prod_id,
	count(*)
from silver.crm_prod_info
group by prod_id
having count(*) <1 or prod_id IS null

---Check unwanted spaces
select prod_name
from silver.prod_info
where prod_name != TRIM(prod_name) --Find any unmactching after trimming

---Check for nulls or unwanted numbers in cost
select prod_cost
from silver.crm_prod_info
where prod_cost IS NULL or prod_cost < 0


--Data Standardization & Consistency
SELECT DISTINCT	prod_line
from silver.crm_prod_info

--Check Invalid Dates
select * 
from silver.crm_prod_info
where prod_end_date < prod_start_date  --end < start


  
------Sales Table-------------------------
---Validate silver
--order date always < ship/due date
select * from silver.crm_sales_details
where sales_order_dt !< sales_due_dt or sales_order_dt !<
sales_ship_dt

--order date < ship date check
select * from silver.crm_sales_details
where sales_cust_id NOT IN (select cust_id from silver.crm_cust_info)


------Sales Table----------------------------------------
--Value <0 or <=0
--check length (8)
--check outliers by validating boundaries of date range
--Check invalid dates
select 
		nullif(	sales_order_dt,0)	sales_order_dt
from silver.crm_sales_details
where sales_due_dt <=0 or
	len(sales_due_dt)!=8 or
	sales_due_dt > 20500101 or
	sales_due_dt < 19000101
 --start of business dates or higher than operating
--32154 and 5489


---Check Data Consistency: sales = qty *price
---sales price no negatives, zero and nulls
select distinct
	sls_sales,
	sales_qty,
	sales_price
from silver.crm_sales_details
where sls_sales != sales_price * sales_qty 
		or	sls_sales is null or sales_qty is null or sales_price is null
		or sls_sales <= 0 or sales_qty <=0 or sales_price <=0
order by sls_sales, sales_qty, sales_price

--sql: fix direct in source system
--Data issues fixed in dwh
--if sales negative, zero, nul derive from qty * price
--if price is zero or null, calculate using sales and qty
--negative value convert to positive




---ERP.CUSTOMER DETAILS TABLE-----------------------------
	--search birthdate OUT of range
	
	select distinct  birthdate 
	from silver.erp_cust_az12
	where birthdate < '1924-01-30' or birthdate > GETDATE()


	--Data Std & Consistency
select distinct gender
from silver.erp_cust_az12
	
	-----LOC Table-------------------------------------------------
	--Invalid values, normalize and handle missing, blank values
	--search unmatching data 
	--where REPLACE(country_id, '-', '') not in 
	--(SELECT cust_key FROM silver.crm_cust_info)

	--where country_id not in 
	--(SELECT cust_key FROM silver.crm_cust_info)


	--Data Std & Consistency
	SELECT distinct country
	FROM silver.erp_loc_a101
	order by country


	--Category Table-----------------------------------------------------
		--Check unwnated spaces -> none
	select * from bronze.erp_px_cat_g1v2
	where Category != TRIM(category) or Subcategory != TRIM(Subcategory)
	or  Maintenance != TRIM(Maintenance)

	--data std & consistency
	select distinct Maintenance
	from bronze.erp_px_cat_g1v2
