/* 
=======================================================================
	Stored Procedure : Load Bronze Layer (Source -> Bronze)
=======================================================================
Purpose : 
  This SP loads data into the 'bronze' schema from external csv file. 
  It performs the following actions:
  - Truncate the bronze table before loading data 
  - used the BULK INSERT to load data from csv file 

Parameter : 
  None. This SP does not accept parameter or return any value. 

Usage :
  run 'EXAC bronze.load_bronze'

Notes : 
	a) Bulk Insert -> Opsi untuk menambahkan data dalam jumlah banyak lebih efisien dan cepat 
dengan menyambungkannya langsung dengan file melalui path.
	- FIRSTROW			: Parameter untuk menentukan bahwa data dimulai pada row kedua
	- FIELDTERMINATOR	: Delimiter/Separator, Lambang yang memisahkan antar kolomnya. 
						Di dalam table karena data dari file .csv maka delimiternya adalah koma. 
	b) TRUNCATE -> Fungsi untuk mengosongkan table atau menghapus seluruh data dalam table
	c) Stored Procedure -> Seperti User Define Function
	d) PRINT -> Menambahkan ini bisa membantu track execution atau debug issue
		- TRY ... CATCH       : track apabila terjadi error
		- Track ETL Duration  : mendeteksi issue dan mengoptimasi performa, terlebih ketika data yang di load besar dan 
                          membutuhkan waktu yang cukup lama
=======================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================';
		PRINT 'Load Bronze Layer';
		PRINT '======================================';

		PRINT '--------------------------------------';
		PRINT 'Loading Table CRM';
		PRINT '--------------------------------------';
	
		-- Menambahkan data ke Table Customer Info
		SET @start_time = GETDATE();
		PRINT 'Truncate Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; 

		PRINT 'Insert Data to Table : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambahkan data ke Table Produk Info
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Insert Data to Table : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambahkan data ke Table Sales Detail 
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : bronze.crm_sales_detail';
		TRUNCATE TABLE bronze.crm_sales_detail;

		PRINT 'Insert Data to Table : bronze.crm_sales_detail';
		BULK INSERT bronze.crm_sales_detail
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		PRINT '--------------------------------------';
		PRINT 'Loading Table ERP';
		PRINT '--------------------------------------';

		-- Menambahkan data ke Table ERP Customer Additional Information
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT 'Insert Data to Table : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		-- Menambah data ke Table ERP Location 
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Insert Data to Table : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';


		-- Menambah data ke Table ERP Category
		SET @start_time = GETDATE();

		PRINT 'Truncate Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Insert Data to Table : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ASUS\Documents\SQL Server Management Studio 22\build-data-warehouse\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
		PRINT '>>.....................';

		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
		PRINT '========================================';
	END TRY

	BEGIN CATCH
	PRINT '========================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '======================================';
	END CATCH

END;
