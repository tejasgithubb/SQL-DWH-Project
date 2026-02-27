--==========================================================
--THIS SECTION HAS DEFINITION STATEMENTS FOR ALL THE SIX TABLES FROM THE ORIGINAL SOURCES OF DATA
--==========================================================



-- Defining all the tables in bronze layer

IF OBJECT_ID('Bronze.brz_crm_cust_info', 'U') IS NOT NULL
   DROP TABLE Bronze.brz_crm_cust_info;

CREATE TABLE Bronze.brz_crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname VARCHAR(60),
	cst_lastname VARCHAR(60),
	cst_marital_status VARCHAR(10),
	cst_gender VARCHAR(10),
	cst_create_date DATE
);


IF OBJECT_ID('Bronze.brz_crm_prd_info', 'U') IS NOT NULL
   DROP TABLE Bronze.brz_crm_prd_info;
CREATE TABLE Bronze.brz_crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_name VARCHAR(60),
	prd_cost INT ,
	prd_line VARCHAR(100) ,
	prd_start_date DATE,
	prd_end_date DATE
);


IF OBJECT_ID('Bronze.brz_crm_sales_details', 'U') IS NOT NULL
   DROP TABLE Bronze.brz_crm_sales_details;
CREATE TABLE Bronze.brz_crm_sales_details (
	sls_ord_num NVARCHAR(15),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_date INT,
	sls_due_date INT ,
	sls_sales INT , 
	sls_quantity INT ,
	sls_price INT 
);

IF OBJECT_ID( 'Bronze.brz_erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE  Bronze.brz_erp_cust_az12;
CREATE TABLE Bronze.brz_erp_cust_az12(
	cust_az12_cid NVARCHAR(50),
	cust_az12_bdate DATE,
	cust_az12_gender VARCHAR(20),
);

IF OBJECT_ID( 'Bronze.brz_erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE  Bronze.brz_erp_loc_a101;
CREATE TABLE Bronze.brz_erp_loc_a101(
	loc_a101_cid NVARCHAR(50),
	loc_a101_country NVARCHAR(50),
);

IF OBJECT_ID( 'Bronze.brz_erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE  Bronze.brz_erp_px_cat_g1v2;
CREATE TABLE Bronze.brz_erp_px_cat_g1v2(
	px_cat_g1v2_id NVARCHAR(50),
	px_cat_g1v2_category NVARCHAR(50),
	px_cat_g1v2_subcategory VARCHAR(50),
	px_cat_g1v2_maintenance VARCHAR(100)
);
