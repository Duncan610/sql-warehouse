/*
================================================================================
DDL Script: Create Silver Tables

Script Purpose:
  Creates tables in the silver schema, dropping tables if they already exist

Warning:
  - Running this script will remove existing data and replace the table structures.
================================================================================
*/

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(100),
	cst_lastname VARCHAR(100),
	cst_marital_status VARCHAR(20),
	cst_gndr VARCHAR(10),
	cst_create_date DATE
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(10),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(255),
	prd_cost NUMERIC(10,2),
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_quantity INT,
	sls_sales NUMERIC(12,2),
	sls_price NUMERIC(10,2)
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(10)
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(100),
	subcat VARCHAR(100),
	maintenance VARCHAR(100)
);
