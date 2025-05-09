/* This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
*/
----CRM DDL----------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'user') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
	cust_id INT,
	cust_key NVARCHAR(50),
	cust_firstname NVARCHAR(50),
	cust_lastname NVARCHAR(50),
	cust_marital_status NVARCHAR(50),
	cust_gender NVARCHAR(50),
	cust_create_date DATE
);



IF OBJECT_ID('bronze.crm_prod_info', 'user') IS NOT NULL
	DROP TABLE bronze.crm_prod_info
CREATE TABLE bronze.crm_prod_info(
	prod_id  INT,
	prod_key NVARCHAR(50),
	prod_name NVARCHAR(50),
	prod_cost INT,
	prod_line NVARCHAR(50),
	prod_start_date DATETIME,
	prod_end_date DATETIME
)

IF OBJECT_ID('bronze.crm_sales_details', 'user') IS NOT NULL
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details(
	sales_order_num NVARCHAR(50),
	sales_prod_key NVARCHAR(50),
	sales_cust_id INT,
	sales_order_dt INT,
	sales_ship_dt INT,
	sales_due_dt INT,
	sls_sales INT,
	sales_qty INT,
	sales_price INT
)

--------------------------------------DDL For ERP System--------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'user') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
    country_id    NVARCHAR(50),
    country  NVARCHAR(50)
)


IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'user') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
	ID NVARCHAR(50),
	Category NVARCHAR(50),
	Subcategory NVARCHAR(50),
	Maintenance NVARCHAR(50)
)


IF OBJECT_ID('bronze.erp_cust_az12', 'user') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
	cust_id NVARCHAR(50),
    birthdate  DATE,
    gender    NVARCHAR(50)
)

