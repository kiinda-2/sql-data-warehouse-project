/*==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
    This performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Usage - Run this query:
    EXEC Silver.load_silver;
*/


--SP responsible: loading silver layer
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		--INSERT DATA INTO TABLE
		--ERP, CRM DATA
		--TRUNCATE(Delete all rows from table, reset to empty state (full load)
		SET @batch_start_time = GETDATE();
		PRINT '------------------------------------------------------'
		PRINT 'Loading Silver  Layer'
		PRINT '------------------------------------------------------'

		PRINT '------------------------------------------------------'
		PRINT 'Load CRM Tables'
		PRINT '------------------------------------------------------'


      
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table:silver.crm_cust_ino'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Inserting Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
				cust_id, 
				cust_key, 
				cust_firstname, 
				cust_lastname, 
				cust_marital_status, 
				cust_gender,
				cust_create_date
			)
		SELECT 
				cust_id,
				cust_key,
				TRIM(cust_firstname) as cust_firstname,
				TRIM(cust_lastname) as cust_lastname,
				CASE WHEN upper(TRIM(cust_marital_status)) = 'S' THEN 'Single'
					 WHEN UPPER(TRIM(cust_marital_status)) = 'M' THEN 'Married'
					 ELSE 'n/a'
				END cust_marital_status,--Normalize marital status to readable format
				CASE WHEN upper(TRIM(cust_gender)) = 'F' THEN 'Female'
					 WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
					 ELSE 'n/a'
				END AS cust_gender, --Normalize Gender 
				cust_create_date
		FROM (
				select *,
				ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) as flag_last
				from bronze.crm_cust_info 
				where cust_id IS NOT NULL
			) t 
      -- Select the most recent record per customer
			where flag_last = 1;
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		

		--prod table
		SET @batch_start_time = GETDATE();
		PRINT '>>Truncating Table:silver.crm_prod_info'
		TRUNCATE TABLE silver.crm_prod_info;

		PRINT '>>Inserting Data Into: silver.crm_prod_info'
		INSERT INTO silver.crm_prod_info (
			prod_id,
			cat_id,
			prod_key,
			prod_name, 
			prod_cost,
			prod_line,
			prod_start_date,
			prod_end_date
		)
		SELECT
			prod_id,
			REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID and replace - with _
			SUBSTRING(prod_key, 7, LEN(prod_key)) AS prod_key, -- Extract product key
			prod_name,
			ISNULL(prod_cost, 0) AS prod_cost,
			CASE 
				WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prod_line, --Normalize Abbreviation to descriptive values
			CAST(prod_start_date AS DATE) AS prod_start_date,
      -- Calculate end date as one day before the next start date
			CAST(DATEADD(DAY, -1, LEAD(prod_start_date) OVER (PARTITION BY prod_key ORDER BY prod_start_date)) AS DATE) AS prod_end_date
		FROM bronze.crm_prod_info;
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		

		--sales table
		SET @batch_start_time = GETDATE();
		PRINT '>>Truncating Table:silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>>Inserting Data Into:silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details
		(
			sales_order_num,
			sales_prod_key,
			sales_cust_id,
			sales_order_dt,
			sales_ship_dt,
			sales_due_dt,
			sls_sales,
			sales_qty,
			sales_price
		)
		select 
			sales_order_num,
			sales_prod_key,
			sales_cust_id,
			case when sales_order_dt =0 or len(sales_order_dt) !=8 then NULL	
				 else cast(cast(sales_order_dt as varchar) as date)
			end as sales_order_date,
			--future mitigation
			case when sales_ship_dt =0 or len(sales_ship_dt) !=8 then NULL	
				 else cast(cast(sales_ship_dt as varchar) as date)
			end as sales_ship_date,
			case when sales_due_dt =0 or len(sales_due_dt) !=8 then NULL	
				 else cast(cast(sales_due_dt as varchar) as date)
			end as sales_due_dt,
			case when sls_sales is null or sls_sales <0 or sls_sales != (sales_qty * abs(sales_price))
					then sales_qty * ABS(sales_price)
				else sls_sales
			end as sales,-- Recalculate sales if original value is missing or incorrect
			sales_qty,
			case when sales_price is null or sales_price <=0
				then sls_sales / nullif(sales_qty,0)--when zero replace with null
				else sales_price      -- Derive price if original value is invalid
			end as sales_price 
		from bronze.crm_sales_details
		where sales_order_num != TRIM(sales_order_num)

		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		

			
		PRINT '------------------------------------------------------'
		PRINT 'Load ERP Data'
		PRINT '------------------------------------------------------'
		---ERP DATA
		SET @batch_start_time = GETDATE();
		PRINT '>>Truncating Table:silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>>Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cust_id, birthdate, gender
		)
		SELECT 
			CASE WHEN cust_id LIKE 'NAS%' then SUBSTRING(cust_id,4,LEN(cust_id))
				else cust_id
			end as 	cust_id,
			case when birthdate > GETDATE() then null 
				else birthdate
			end as birthdate, -- Set future birthdates to NULL
			case when UPPER(TRIM(gender)) IN ('F', 'FEMALE') then 'Female'
				when UPPER(TRIM(gender)) IN ('MALE', 'M') then 'Male'
				else 'n/a'
			end as gender -- Normalize gender values and handle unknown cases
		from bronze.erp_cust_az12
		--from  silver.erp_cust_az12
		where CASE WHEN cust_id LIKE 'NAS%' then SUBSTRING(cust_id,4,LEN(cust_id))
				else cust_id
			end not in(select distinct cust_key 	from silver.crm_cust_info
			)
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		
		
		---erp.location table
		SET @batch_start_time = GETDATE();
		PRINT '>>Truncating Table:silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>>Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
			cid,	country
			)
		SELECT 
			REPLACE(country_id, '-', '') as country_id,
			case when TRIM(country) = 'DE' then 'Germany'
				when TRIM(country) IN ('US', 'USA') then 'United States'
				when TRIM(country) IS NULL or TRIM(country) = '' then 'n/a'
				else country
			end as country -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101
	
		SET @end_time =  GETDATE();
		PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
		PRINT '----------------------------------------------------------------'
		

		---cat table
		SET @batch_start_time = GETDATE();
		PRINT '>>Truncating Table:silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>>Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			Category,
			Subcategory,
			Maintenance
		)
		select 
			id,
			Category,
			Subcategory,
			Maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time =  GETDATE();
			PRINT'>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'sec';
			PRINT '----------------------------------------------------------------'
	
	
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver 	Layer is Completed';
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
