CREATE OR ALTER PROCEDURE Silver.sp_slv_load_table AS
BEGIN
     
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_crm_cust_info'
	TRUNCATE TABLE Silver.slv_crm_cust_info
	PRINT '>> INSERTING DATA INTO: Silver.slv_crm_cust_info'
	
    
    INSERT INTO Silver.slv_crm_cust_info (cst_id, 
              cst_key, 
              cst_firstname, 
              cst_lastname, 
              cst_marital_status,
              cst_gender,
              cst_create_date)
              
              
              SELECT 
              	cst_id,
              	cst_key,
              	TRIM(cst_firstname) AS cst_firstname,
              	TRIM(cst_lastname) AS cst_lastname,
              
              	CASE WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
              		 WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
              		 ELSE 'N/A' 
                  END AS cst_marital_status, 
              
              	CASE WHEN UPPER(cst_gender) = 'M' THEN 'Male'-- normalisation and standardisation
              		 WHEN UPPER(cst_gender) = 'F' THEN 'Female'
              		 ELSE 'N/A' --missing data handling
              	END cst_gender,
              	cst_create_date
              
              FROM
              	(SELECT
              	*,
              	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC)AS occurance_flag -- removeing duplicates
              	FROM Data_Warehouse.Bronze.brz_crm_cust_info
              	WHERE cst_id IS NOT NULL
              	) t
              	WHERE t.occurance_flag = 1

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


-------------------------------------------------------------------------------
    
    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_crm_prd_info'
	TRUNCATE TABLE Silver.slv_crm_prd_info
	PRINT '>> INSERTING DATA INTO: Silver.slv_crm_prd_info'
    
    INSERT INTO Silver.slv_crm_prd_info(
                	prd_id,
                	prd_category_id,
                	prd_key,
                	prd_name,
                	prd_cost,
                	prd_line,
                	prd_start_date,
                	prd_end_date)
                
                
                SELECT
                prd_id,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_category_id ,-- data refining
                SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
                prd_name,
                ISNULL(prd_cost, 0) AS prd_cost,--replace null values with 0
                CASE  UPPER(TRIM(prd_line))   --quicker way
                	WHEN  'M' THEN 'Mountain'
                	WHEN  'S' THEN 'Sport'
                	WHEN  'R' THEN 'Road'-- normalisation and standardisation
                    WHEN  'T' THEN 'Tour'
                	ELSE 'N/A' 
                END AS prd_line, 
                CAST(prd_start_date AS DATE) AS prd_start_date  ,
                CAST(DATEADD(DAY, 
                			-1, 
                             LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)
                			 ) AS DATE) AS prd_end_date -- to ensure that there is no overlap among start and end dates -- fix the dates and maintain consistency
                FROM Bronze.brz_crm_prd_info

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
-------------------------------------------------------------------------------

    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_crm_sales_details'
	TRUNCATE TABLE Silver.slv_crm_sales_details
	PRINT '>> INSERTING DATA INTO: Silver.slv_crm_sales_details'
     
    INSERT INTO silver.slv_crm_sales_details(
                    sls_ord_num,     
                    sls_prd_key ,    
                    sls_cust_id  ,  
                    sls_order_date,  
                    sls_ship_date  ,
                    sls_due_date ,
                    sls_sales     ,  
                    sls_quantity,
                    sls_price )
                
                
                
                SELECT  
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                CASE WHEN sls_order_date = 0 OR LEN(sls_order_date)!=8 THEN NULL 
                     ELSE CAST (CAST ( sls_order_date AS VARCHAR) AS DATE)--cant be casted directly to date from int -- cast integrer into date
                     END AS sls_order_date,
                
                
                CASE WHEN sls_ship_date = 0 OR LEN(sls_ship_date)!=8 THEN NULL 
                     ELSE CAST (CAST ( sls_ship_date AS VARCHAR) AS DATE)--cant be casted directly to date from int
                     END AS sls_ship_date,
                
                
                CASE WHEN sls_due_date = 0 OR LEN(sls_due_date)!=8 THEN NULL 
                     ELSE CAST (CAST ( sls_due_date AS VARCHAR) AS DATE)--cant be casted directly to date from int
                END AS sls_due_date,
                
                CASE WHEN sls_sales IS NULL OR sls_sales < = 0 OR sls_sales != ABS(sls_price*sls_quantity)  -- correct sales and price que=antity relations
                     THEN ABS(sls_price*sls_quantity)
                     ELSE sls_sales
                END AS sls_sales,
                
                sls_quantity,
                
                CASE WHEN sls_price IS NULL OR sls_price < = 0  
                     THEN sls_sales/NULLIF(sls_quantity,0) 
                     ELSE sls_price
                END AS sls_price 
                
                
                FROM Data_Warehouse.Bronze.brz_crm_sales_details
                
           

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
-------------------------------------------------------------------------------

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_erp_cust_az12'
	TRUNCATE TABLE Silver.slv_erp_cust_az12
	PRINT '>> INSERTING DATA INTO: Silver.slv_erp_cust_az12'
  
    INSERT INTO Silver.slv_erp_cust_az12(cust_az12_cid, cust_az12_bdate, cust_az12_gender)
                
                SELECT 
                CASE WHEN cust_az12_cid LIKE 'NAS%' --Remove NAS prefix
                     THEN SUBSTRING(cust_az12_cid, 4, LEN(cust_az12_cid))
                     ELSE cust_az12_cid
                     END AS cust_az12_cid,

                CASE WHEN cust_az12_bdate > GETDATE() THEN NULL
                     ELSE cust_az12_bdate
                END AS cust_az12_bdate, -- set future birth dates to null

                CASE WHEN UPPER(TRIM(cust_az12_gender)) IN ('F','FEMALE') THEN 'Female'
                     WHEN UPPER(TRIM(cust_az12_gender)) IN ('M','MALE') THEN 'Male'
                     ELSE 'N/A' -- data normalisation and standardisation
                END AS cust_az12_gender
                
                FROM Bronze.brz_erp_cust_az12


                                                                                /*WHERE cust_az12_bdate < '1924-01-01' OR cust_az12_bdate > GETDATE()
                                                                                
                                                                                SELECT DISTINCT cust_az12_gender FROM  Bronze.brz_erp_cust_az12
                                                                                
                                                                                WHERE CASE WHEN cust_az12_cid LIKE 'NAS%'
                                                                                     THEN SUBSTRING(cust_az12_cid, 4, LEN(cust_az12_cid))
                                                                                     ELSE cust_az12_cid END NOT IN (SELECT cst_key FROM Bronze.brz_crm_cust_info)*/ -- Check for mismatch with customer crm tab*/
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

-------------------------------------------------------------------------------

    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_erp_loc_a101'
	TRUNCATE TABLE Silver.slv_erp_loc_a101
	PRINT '>> INSERTING DATA INTO: Silver.slv_erp_loc_a101'
   
   INSERT INTO Silver.slv_erp_loc_a101(loc_a101_cid, loc_a101_country)
                
                SELECT 
                REPLACE (loc_a101_cid, '-', '') AS loc_a101_cid  , -- removing extra dashes

                CASE WHEN UPPER(TRIM(loc_a101_country)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States' 
                     WHEN UPPER(TRIM(loc_a101_country)) = 'DE' THEN 'Germany'
                     WHEN TRIM(loc_a101_country) = '' OR loc_a101_country IS NULL THEN 'N/A'
                     ELSE loc_a101_country   -- data normalisation and standardisation
                END AS loc_a101_country

                FROM Bronze.brz_erp_loc_a101

                                                                                --WHERE REPLACE (loc_a101_cid, '-', '')  NOT IN (SELECT cst_key FROM Bronze.brz_crm_cust_info ) to check if any data remains
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
------------------------------------------------------------------------------

    SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE: Silver.slv_erp_px_cat_g1v2'
	TRUNCATE TABLE Silver.slv_erp_px_cat_g1v2
	PRINT '>> INSERTING DATA INTO: Silver.slv_erp_px_cat_g1v2'
    
    INSERT INTO Silver.slv_erp_px_cat_g1v2(
                  			px_cat_g1v2_id,  
                  			px_cat_g1v2_category,
                  			px_cat_g1v2_subcategory,
                  			px_cat_g1v2_maintenance
                  			)
                  SELECT 
                  px_cat_g1v2_id, -- Already verified earlier in silver layer creation of product info
                  px_cat_g1v2_category,
                  px_cat_g1v2_subcategory,
                  px_cat_g1v2_maintenance
                  FROM Bronze.brz_erp_px_cat_g1v2

                                                                       -- this table has no unwanted spaces. all the categories, subcategories and maintenece data qualities are fine. this can be loaded straight away
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
------------------------------------------------------------------------------
END
