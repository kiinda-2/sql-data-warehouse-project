--===============================================================================
--DDL Script: Create Silver Tables
--===============================================================================
    --This script creates tables in the 'silver' schema, dropping existing tables 
    --if they already exist.
	  --Run this script to re-define the DDL structure of 'bronze' Tables
--===============================================================================

----CRM TABLES-------------------------------------------------------------------
--Drop table if exists, then create table from scratch
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	BEGIN
		DROP TABLE silver.crm_cust_info
	END
CREATE TABLE silver.crm_cust_info(
	cust_id INT,
	cust_key NVARCHAR(50),
	cust_firstname NVARCHAR(50),
	cust_lastname NVARCHAR(50),
	cust_marital_status NVARCHAR(50),
	cust_gender NVARCHAR(50),
	cust_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
		);


IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_prod_info;
END

CREATE TABLE silver.crm_prod_info (
    prod_id INT,
    cat_id NVARCHAR(50),
    prod_key NVARCHAR(50),
    prod_name NVARCHAR(50),
    prod_cost INT,
    prod_line NVARCHAR(50),
    prod_start_date DATE,
    prod_end_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


--after cleaning updating ddl
--date changed -> int to date, so update data type structure
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details
GO
CREATE TABLE silver.crm_sales_details(
	sales_order_num NVARCHAR(50),
	sales_prod_key NVARCHAR(50),
	sales_cust_id INT,
	sales_order_dt DATE,
	sales_ship_dt DATE,
	sales_due_dt DATE,
	sls_sales INT ,
	sales_qty INT,
	sales_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)


------ERP TABLES----------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101
GO
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    country  NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)


IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2
GO
CREATE TABLE silver.erp_px_cat_g1v2(
	ID NVARCHAR(50),
	Category NVARCHAR(50),
	Subcategory NVARCHAR(50),
	Maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)


IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12
GO
CREATE TABLE silver.erp_cust_az12(
	cust_id NVARCHAR(50),
    birthdate  DATE,
    gender    NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
