/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================*/
--customer dimension
CREATE OR ALTER VIEW Gold.dim_customers AS
--SELECT COUNT(cst_id) FROM check for data duplicacy

SELECT  ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gender !='N/A' THEN ci.cst_gender
		     ELSE COALESCE(ca.cust_az12_gender, 'N/A')
		END AS gender,
		ci.cst_create_date AS create_date,
		ca.cust_az12_bdate AS birth_date,
		lo.loc_a101_country AS country
FROM Silver.slv_crm_cust_info AS ci
LEFT JOIN Silver.slv_erp_cust_az12 AS ca
ON ci.cst_key=ca.cust_az12_cid
LEFT JOIN Silver.slv_erp_loc_a101 AS lo
ON lo.loc_a101_cid=ci.cst_key

--AS t
--GROUP BY cst_id
--HAVING COUNT(cst_id)>1

--===============================================
--product dimension view
CREATE OR ALTER VIEW Gold.dim_products AS

SELECT  ROW_NUMBER() OVER(ORDER BY prc.prd_start_date, prc.prd_key) AS product_key,
		prc.prd_id AS product_id,
		prc.prd_key AS product_number,
		prc.prd_name AS product_name,
		prc.prd_category_id AS product_category_id,
		pre.px_cat_g1v2_category AS product_category,
		pre.px_cat_g1v2_subcategory AS subcategory,
		pre.px_cat_g1v2_maintenance AS maintenance,
		prc.prd_cost AS product_cost,
		prc.prd_line AS product_line,
		prc.prd_start_date AS [start_date]
		
		
	
		
FROM Silver.slv_crm_prd_info AS prc
LEFT JOIN Silver.slv_erp_px_cat_g1v2 AS pre 
ON prc.prd_category_id = pre.px_cat_g1v2_id
WHERE prc.prd_end_date IS NULL
--========================================

--sales facts
  
CREATE OR ALTER VIEW Gold.fact_sales AS

SELECT

    sd.sls_ord_num AS order_number,
    pd.product_key,
    cs.customer_key,
    sd.sls_order_date AS order_date,
    sd.sls_ship_date AS shipping_date,
    sd.sls_due_date AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM Silver.slv_crm_sales_details AS sd
LEFT JOIN Gold.dim_customers AS cs
ON sd.sls_cust_id = cs.customer_id
LEFT JOIN Gold.dim_products AS pd
ON sd.sls_prd_key = pd.product_number

