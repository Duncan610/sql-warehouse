/*
================================================================================
Stored Procedure: sp_clean_and_load_silver_tables

Purpose:
  Performs ETL operations to load cleaned, deduplicated, and standardized data 
  from the 'bronze' (raw) schema into the 'silver' (curated) schema for use in 
  analytics and reporting.

Process Overview:
  - Truncates existing records in silver tables to prevent duplicates.
  - Applies transformations such as trimming text, standardizing values, 
    deduplicating rows using window functions, and correcting invalid entries.
  - Joins and filters data where necessary to maintain consistency across CRM 
    and ERP datasets.
  - Includes error handling to raise a notice if the ETL process fails.

Tables Processed:
  - crm_cust_info
  - crm_prd_info
  - crm_sales_details
  - erp_cust_aziz12
  - erp_loc_a101
  - erp_px_cat_g1v2

Usage:
  CALL silver.sp_clean_and_load_silver_tables();

Warning:
  - This procedure **truncates** existing data in silver tables before loading.
================================================================================
*/

DROP PROCEDURE IF EXISTS silver.sp_clean_and_load_silver_tables;
CREATE PROCEDURE silver.sp_clean_and_load_silver_tables()
LANGUAGE plpgsql
AS $$
BEGIN

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'F' THEN 'Female'
        WHEN 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1;

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
    SUBSTRING(prd_key FROM 7),
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    prd_start_dt::DATE,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') NOT IN (
    SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
);

TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
    sls_ship_dt, sls_due_dt, sls_quantity, sls_sales, sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL ELSE sls_order_dt END,
    CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL ELSE sls_ship_dt END,
    CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL ELSE sls_due_dt END,
    sls_quantity,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;

TRUNCATE TABLE silver.erp_cust_aziz12;
INSERT INTO silver.erp_cust_aziz12 (
    cid, cid_cleaned, bdate, gen_cleaned
)
SELECT
    cid,
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) ELSE cid END,
    CASE WHEN bdate > CURRENT_TIMESTAMP THEN NULL ELSE bdate END,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
    cid, cntry
)
SELECT
    REPLACE(cid, '-', ''),
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;

TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (
    id, cat, subcat, maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during ETL: %', SQLERRM;
END;
$$;
