/*
=================================================
	DDL Script : Create View on Gold Schema 
=================================================
Script Purpose : 
	This script create views on gold schema; 
	final dimension and fact table (Star Schema).
	
	Each views performs transformation and combines from the silver layer 
	to produce a clean, enriched, and bussiness-ready dataset.

Usage : 
	directly for analytics and reporting 
==================================================
*/

---------------------------------------------------
--	Create Dimension : gold.dim_customers
---------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO 

CREATE VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key (PK in Dim table) / FK in Fact Table
		ci.cst_id			AS customer_id,
		ci.cst_key			AS customer_number,
		ci.cst_firstname	AS first_name,
		ci.cst_lastname		AS last_name,
		lo.cntry,
		ci.cst_material_status AS martial_status,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
			ELSE COALESCE(ca.gen, 'n/a') -- if, gender on CRM customer info = 'n/a' then get info gender from ERP
		END AS gender, 
		ca.bdate			AS birthdate, 
		ci.cst_create_date	AS create_date
	FROM silver.crm_cust_info as ci
	LEFT JOIN silver.erp_cust_az12 as ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 as lo
	ON ci.cst_key = lo.cid

---------------------------------------------------
--	Create Dimension : gold.dim_products
---------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO 

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_date, pn.prd_key) AS product_key, -- Surrogate key / FK
	pn.prd_id		AS product_id,
	pn.prd_key		AS product_number,
	pn.prd_nm		AS product_name,
	pn.cat_id		AS category_id,
	pc.cat			AS category,
	pc.subcat		AS subcategory,
	pc.maintenance	AS maintenance,
	pn.prd_cost		AS product_cost,
	pn.prd_line		AS product_line,
	pn.prd_start_date AS start_date
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_date IS NULL

---------------------------------------------------
--	Create Fact : gold.fact_sales
---------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL 
	DROP VIEW gold.fact_sales;
GO 

CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num	AS order_number, -- Primary Key 
	cu.customer_key AS customer_key,
	pr.product_key	AS product_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt	AS shipping_date,
	sd.sls_due_dt	AS due_date,
	sd.sls_sales	AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price	AS price
FROM silver.crm_sales_detail as sd
LEFT JOIN gold.dim_customers as cu 
ON sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products as pr
ON sd.sls_prd_key = pr.product_number 
