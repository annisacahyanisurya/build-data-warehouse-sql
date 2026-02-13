/* 
=======================================================================
	Stored Procedure : Load Silver Layer (Bronze -> Silver)
=======================================================================
Purpose : 
  This SP performes the ETL (Extract, Transform, Load) process to 
  populate the silver schema from the bronze schema. 
  It performs the following actions:
  - Truncate the silver table before loading data 
  - Insert, Transform, and Cleaned data into silver layer. 

Parameter : 
  None. This SP does not accept parameter or return any value. 

Usage :
  run 'EXAC silver.load_silver'

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================';
		PRINT 'Load Silver Layer';
		PRINT '======================================';

		PRINT '--------------------------------------';
		PRINT 'Loading Table CRM';
		PRINT '--------------------------------------';
	
		-- Menambahkan data ke Table Customer Info
		SET @start_time = GETDATE();
		PRINT 'Truncate Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info; 

		PRINT 'Insert Data to Table : silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id, 
			cst_key, 
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
		cst_id, 
		cst_key, 
		TRIM(cst_firstname) as cst_firstname, 
		TRIM(cst_lastname) as cst_lastname, 
		CASE WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
			WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_material_status, -- Normalize martial_status values to readalble format
		CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			 WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gndr, -- Normalize customer gender values to readalble format
		cst_create_date
		FROM (	
			SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
			FROM bronze.crm_cust_info	
			WHERE cst_id IS NOT NULL 
		)t WHERE flag_last = 1


		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambahkan data ke Table Produk Info
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT 'Insert Data to Table : silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id, 
			cat_id, 
			prd_key, 
			prd_nm, 
			prd_cost, 
			prd_line, 
			prd_start_date, 
			prd_end_date
		)
		SELECT 
		prd_id, 
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- REPLACE untuk menyamakan format dengan table erp category
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm, 
		ISNULL(prd_cost, 0) AS prd_cost, -- mengubah NULL -> 0
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, 
		CAST(prd_start_date AS DATE) AS prd_start_date,
		CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)-1 AS DATE) AS prd_end_date 
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambahkan data ke Table Sales Detail 
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : silver.crm_sales_detail';
		TRUNCATE TABLE silver.crm_sales_detail;

		PRINT 'Insert Data to Table : silver.crm_sales_detail';
		INSERT INTO silver.crm_sales_detail(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id ,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN CAST(sls_order_dt AS VARCHAR) = '0' OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 
				THEN NULL
				ELSE TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN CAST(sls_ship_dt AS VARCHAR) = '0' OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 
				THEN NULL
				ELSE TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN CAST(sls_due_dt AS VARCHAR) = '0' OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 
				THEN NULL
				ELSE TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- if Sales is negative, zero, or null derive it using Quantity or Price
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			-- if Price is zero or null, calculate it using Sales and Quantity OR Price is negative, covert to possitive
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_detail

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		PRINT '--------------------------------------';
		PRINT 'Loading Table ERP';
		PRINT '--------------------------------------';

		-- Menambahkan data ke Table ERP Customer Additional Information
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT 'Insert Data to Table : silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid, 
			bdate, 
			gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
			ELSE cid
		END AS cid, 
		CASE WHEN bdate > GETDATE() THEN NULL 
			ELSE bdate
		END AS bdate, 
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambah data ke Table ERP Location 
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT 'Insert Data to Table : silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid, 
			cntry
		)
		SELECT 
		REPLACE(cid, '-', '') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';


		-- Menambah data ke Table ERP Category
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT 'Insert Data to Table : silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id, 
			cat, 
			subcat, 
			maintenance
		)
		SELECT 
		id, 
		cat, 
		subcat, 
		maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
		PRINT '========================================';
	END TRY

	BEGIN CATCH
	PRINT '========================================';
	PRINT 'ERROR OCCURED DURING LOADING Silver LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '======================================';
	END CATCH

END;
