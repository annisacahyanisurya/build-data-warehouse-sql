/*
=====================================================
    QUALITY CHECKS
=====================================================
Script Purpose:
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
=====================================================
*/

-- ------------------------------------------------
-- Checking : silver.crm_cust_info 
-- ------------------------------------------------

-- 1. Check For Nulls or Duplicates in Primary Key 
-- Expectation : No Result 
SELECT 
    cst_id, COUNT(*) 
FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- 2. Check for Unwanted Spaces (checking For cst_firstname, cst_lastname)
-- Expectation : No Result
SELECT 
    cst_firstname
FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

-- 3. Data Standarization/Normalization and Consistency (+ Handling Missing Data) 
-- Change M to Married and S to Single in crm_material_status | Change F to Female and M to Male | Add missing data to default 'n/a'
-- Expectation : No Result 
SELECT DISTINCT 
    cst_material_status, cst_gndr 
FROM silver.crm_cust_info


-- ------------------------------------------------
-- Checking : silver.crm_prd_info 
-- ------------------------------------------------

-- 1. Check For Nulls or Duplicates in Primary Key 
-- Expectation : No Result 
SELECT 
    prd_id, COUNT(*) 
FROM silver.crm_prd_info 
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- 2. Check for Unwanted Spaces (cheking Product Name)
-- Expectation : No Result
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- 3. Check for NULL or Negative Number
-- Expectation : No Result 
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- 4. Data Standarization & Consistency
-- Expectation: No Results
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info


-- 5. Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date

-- ------------------------------------------------
-- Checking : silver.crm_sales_detail
-- ------------------------------------------------

-- 1. Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_detail
WHERE sls_order_dt <= 0 
    OR LEN(sls_order_dt) != 8 
    OR sls_order_dt > 20500101 
    OR sls_due_dt < 19000101

-- 2. Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_detail
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt


-- 3. Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_detail
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- ------------------------------------------------
-- Checking : silver.erp_cust_az12
-- ------------------------------------------------

-- 1. Identify Out-of-Range Dates
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE 
    bdate < '1924-01-01' 
    OR bdate > GETDATE()

-- 2. Data Standarization & Consistency 
SELECT DISTINCT 
    gen, 
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	    ELSE 'n/a'
    END AS gender
FROM silver.erp_cust_az12



-- ------------------------------------------------
-- Checking : silver.erp_loc_a101
-- ------------------------------------------------

-- 1. Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry



-- ------------------------------------------------
-- Checking : silver.erp_px_cat_g1v2
-- ------------------------------------------------

-- 1. Check Unwanted Spaces
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) 
    OR maintenance != TRIM(maintenance)

-- 2. Data Standarization & Consistency 
SELECT DISTINCT 
    cat
FROM silver.erp_px_cat_g1v2
