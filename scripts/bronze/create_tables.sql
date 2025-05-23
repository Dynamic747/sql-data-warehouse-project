/*
=============================================================
Create Bronze Tables
=============================================================
Script Purpose:
    There are several .csv files we are required to import into the bronze layer, this script creates the DDL for 
    both the source_crm and source_erp tables.
    
    We are going to use the naming convention <schema>.<sourcesystem>_<entity>
    */

DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_martial_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt VARCHAR(8),
sls_ship_dt VARCHAR(8),
sls_due_dt VARCHAR(8),
sls_sales INT,
sls_quantity INT,
sls_price INT
);

/* now for the erp tables */

DROP TABLE IF EXISTS bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12 (
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101 (
CID VARCHAR(50),
CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2 (
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);
