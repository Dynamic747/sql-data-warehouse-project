-- Create Dimension Tables

CREATE OR REPLACE VIEW gold.dim_customers AS

SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    cci.cst_id AS customer_id,
    cci.cst_key AS customer_number,
    cci.cst_firstname AS first_name,
    cci.cst_lastname AS last_name,
    cci.cst_martial_status AS martial_status,
    CASE
        WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
            ELSE COALESCE(eca.gen, 'n/a')
        END AS gender,
    cci.cst_create_date AS create_date,
    eca.bdate AS birth_date,
    ela.cntry AS country
FROM silver.crm_cust_info cci 
LEFT JOIN silver.erp_cust_az12 eca 
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela 
ON cci.cst_key = ela.cid

CREATE OR REPLACE VIEW gold.dim_products AS

SELECT 
ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key,
cpi.prd_id AS product_id,
cpi.cat_id AS category_id,
cpi.prd_key AS product_number,
cpi.prd_nm AS product_name,
epcgv.cat AS category,
epcgv.maintenance AS maintenance ,
epcgv.subcat AS subcategory,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS start_date
FROM 
silver.crm_prd_info cpi 
LEFT JOIN silver.erp_px_cat_g1v2 epcgv 
ON cpi.prd_key = epcgv.id
WHERE prd_end_dt IS NULL

CREATE OR REPLACE VIEW gold.fact_sales AS 

SELECT 
sls_ord_num AS order_number,
dp.product_key,
cu.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales,
sls_quantity AS quantity,
sls_price AS price
FROM silver.crm_sales_details csd 
LEFT JOIN gold.dim_products dp 
ON csd.sls_prd_key = dp.product_number 
LEFT JOIN gold.dim_customers cu
ON csd.sls_cust_id = cu.customer_id
