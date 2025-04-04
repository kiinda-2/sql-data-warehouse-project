-----------------------------DDL For CRM System-------------------------------------------------
/*Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables*/

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
	sls_sales INT ,
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




  
/*Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
 */
  
--STORE FREQENTLY ACCESSED QUERY IN STORED PROCEDURE

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		--INSERT DATA INTO TABLE
		--TRUNCATE(Delete all rows from table, reset to empty state (full load)
    --Calculate execution time also
		SET @batch_start_time = GETDATE();
		PRINT '------------------------------------------------------'
		PRINT 'Loading Bronze Layer'
		PRINT '------------------------------------------------------'

		PRINT '------------------------------------------------------'
		PRINT 'Load CRM Tables'
		PRINT '------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.crm_cust_info table'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>>Inserting data into bronze.crm_cust_info table'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		
		---prod-info
		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.crm_prod_info table'
		TRUNCATE TABLE bronze.crm_prod_info
	
		PRINT '>>Inserting data into bronze.crm_prod_info table'
		BULK INSERT bronze.crm_prod_info
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------------------------------'
		
		---sales-info
		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.crm_sales_details table'
		TRUNCATE TABLE bronze.crm_sales_details
	
		PRINT '>>Inserting data into bronze.crm_sales_details table'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------------------------------'
		
		
		PRINT '------------------------------------------------------'
		PRINT 'Load ERP Data'
		PRINT '------------------------------------------------------'
		--cust_az12
		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.erp_cust_az12 table'
		TRUNCATE TABLE bronze.erp_cust_az12
	
		PRINT '>>Inserting data into bronze.erp_cust_az12 table'
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------------------------------'
		
		--loc_a101
		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.erp_loc_a101table'
		TRUNCATE TABLE bronze.erp_loc_a101
	
		PRINT '>>Inserting data into bronze.erp_loc_a101 table'
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------------------------------'
		
		--px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>>Truncating bronze.erp_px_cat_g1v2 table'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
	
		PRINT '>>Inserting data into bronze.erp_px_cat_g1v2 table'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\data\datasets\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------------------------------'
		
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

		END TRY
		BEGIN CATCH
			PRINT '-----------------------------------------------------'
			PRINT 'LOADING ERROR!'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '-----------------------------------------------------'
		END CATCH
END
