/*
================================================================================
Stored Procedure: prepare_for_load

Purpose:
  - Automates the truncation of all tables in the 'bronze' schema.
  - Ensures clean data loads by resetting identity columns.
  - Provides error-tolerant execution with custom notices for each operation.

Usage:
  CALL bronze.prepare_for_load();

Behavior:
  - Each table truncation is wrapped in an error-handling block to ensure
    the procedure completes even if individual truncations fail.
  - Uses RESTART IDENTITY to reset serial (auto-increment) columns.
================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.prepare_for_load()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Truncating each table with individual error blocks
    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.crm_cust_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.crm_prd_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.crm_sales_details: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12 RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.erp_cust_az12: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101 RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.erp_loc_a101: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2 RESTART IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '⚠️ Failed to truncate bronze.erp_px_cat_g1v2: %', SQLERRM;
    END;

    RAISE NOTICE '✅ Procedure completed successfully. All truncates attempted.';
END;
$$;

-- Execution
CALL bronze.prepare_for_load();
