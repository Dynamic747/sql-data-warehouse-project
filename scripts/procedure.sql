/*
=============================================================
Create Bronze Tables
=============================================================
Script Purpose:
    There are several .csv files we are required to import into the bronze layer, this script inserts 
    the csv files into the layer
            
    We are going to use the copy command in postgres to load the csv data in, using a full load so we will truncate the table,
    we require the procedure to do this too.
    */


CREATE OR REPLACE PROCEDURE bronze.load_bronze()

LANGUAGE plpgsql
AS $$

DECLARE 
    l_ins_crm_cust_info INT;
    l_before_crm_cust_info INT;
    l_after_crm_cust_info INT;

    l_ins_crm_prd_info INT;
    l_before_crm_prd_info INT;
    l_after_crm_prd_info INT;

    l_ins_crm_sales_details INT;
    l_before_crm_sales_details INT;
    l_after_crm_sales_details INT;

    BEGIN

        RAISE NOTICE '=================================';
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '=================================';

        RAISE NOTICE 'Loading CRM Tables';

    -- CRM Customer Info
    BEGIN
        SELECT COUNT(*) INTO l_before_crm_cust_info FROM bronze.crm_cust_info;

        RAISE NOTICE 'Truncate crm_cust_info table';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE 'Insert crm_cust_info table';
        COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_martial_status, cst_gndr, cst_create_date)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
        
        SELECT COUNT(*) INTO l_after_crm_cust_info FROM bronze.crm_cust_info;
        
        l_ins_crm_cust_info:= l_after_crm_cust_info - l_before_crm_cust_info;
        RAISE NOTICE 'Rows inserted into crm_cust_info: %', l_ins_crm_cust_info;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- CRM prd info
    BEGIN
        SELECT COUNT(*) INTO l_before_crm_prd_info FROM bronze.crm_prd_info;

        RAISE NOTICE 'Truncate crm_prd_info table';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE 'Insert crm_prd_info table';    
        COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;

        SELECT COUNT(*) INTO l_after_crm_prd_info FROM bronze.crm_prd_info;

        l_ins_crm_prd_info:= l_after_crm_prd_info - l_before_crm_prd_info;
        RAISE NOTICE 'Rows inserted into crm_prd_info: %', l_ins_crm_prd_info;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;  

    -- CRM sales details
    BEGIN
        SELECT COUNT(*) INTO l_before_crm_sales_details FROM bronze.crm_sales_details;

        RAISE NOTICE 'Truncate crm_sales_details table';
        TRUNCATE TABLE bronze.crm_sales_details;
             
        RAISE NOTICE 'Insert crm_sales_details table';    
        COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;    

        SELECT COUNT(*) INTO l_after_crm_sales_details FROM bronze.crm_sales_details;

        l_ins_crm_sales_details:= l_after_crm_sales_details - l_before_crm_sales_details;
        RAISE NOTICE 'Rows inserted into crm_sales_details: %', l_ins_crm_sales_details;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Rows inserted into crm_sales_details: %', SQLERRM;
    END;

        
        RAISE NOTICE '------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------';

    -- erp cust_az12 table
    BEGIN

        RAISE NOTICE 'Truncate erp_cust_az12 table';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        RAISE NOTICE 'Insert erp_cust_az12 table';    
        COPY bronze.erp_cust_az12 (CID, BDATE, GEN)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION 
        WHEN OTHERS THEN
        RAISE NOTICE 'Error loading cust_az12: %', SQLERRM;
    END;
    
    -- erp_LOC_A101
    BEGIN
        
        RAISE NOTICE 'Truncate erp_LOC_A101 table';
        TRUNCATE TABLE bronze.erp_LOC_A101;
        
        RAISE NOTICE 'Insert erp_LOC_A101 table';    
        COPY bronze.erp_LOC_A101 (CID, CNTRY)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION 
        WHEN OTHERS THEN
        RAISE NOTICE 'Error loading LOC_A101: %', SQLERRM;
    END;

    -- erp_PX_CAT_G1V2
    BEGIN 
        
        RAISE NOTICE 'Truncate erp_PX_CAT_G1V2 table';
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        
        RAISE NOTICE 'Insert erp_PX_CAT_G1V2 table';        
        COPY bronze.erp_PX_CAT_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)
        FROM '/Users/jmanku/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION 
        WHEN OTHERS THEN
        RAISE NOTICE 'Error loading PX_CAT_G1V2: %', SQLERRM;
    END;

END;
$$


