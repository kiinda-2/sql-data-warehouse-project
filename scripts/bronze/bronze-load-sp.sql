/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Usage:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		--INSERT DATA INTO TABLE
		--CRM DATA
		--TRUNCATE(Delete all rows from table, reset to empty state (full load)
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
		---ERP DATA
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
