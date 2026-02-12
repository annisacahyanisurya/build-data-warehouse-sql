/*
================================================================
	DDL Script : Create Tables in Bronze Layer
================================================================
Purpose :
	Create All Table Structure in Bronze Layer and 
	check the availibility of the table.
================================================================
*/

-- Membuat table customer information, but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL -- 'U' stand for User Define, sehingga mencari table yang dibuat oleh user
	DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
	cst_id INT, 
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

-- Membuat table Produk Information, but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
	prd_id INT, 
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT, 
	prd_line NVARCHAR(50),
	prd_start_date DATETIME,
	prd_end_date DATETIME
);

-- Membuat table sales detail (detail penjualan), but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.crm_sales_detail', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_detail;

CREATE TABLE bronze.crm_sales_detail(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
);

-- CREATING TABLES FROM ERP DATA

-- Membuat table location, but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-- Membuat table for additional information about customer, but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE, 
	gen NVARCHAR(50)
);

-- Membuat table, but first of all we have to check the availibility of the table
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
