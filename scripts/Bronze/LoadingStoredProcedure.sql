CREATE OR ALTER PROCEDURE Bronze.sp_brz_load_table AS
--EXEC Bronze.sp_brz_load_table
BEGIN
		 DECLARE @batch_start_time DATETIME, @start_time DATETIME, @end_time DATETIME, @batch_end_time DATETIME;
		 BEGIN TRY

		    SET @batch_start_time = GETDATE();
			PRINT '===================================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '===================================================================';

			PRINT '-------------------------------------------------------------------'
			PRINT 'Loading crm data'
			PRINT '-------------------------------------------------------------------'

		------------


			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING TABLE AND INSERTING INTO Bronze.brz_crm_cust_info'
			TRUNCATE TABLE Bronze.brz_crm_cust_info; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_crm_cust_info
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
			PRINT '--------'
		-------------


			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING TABLE AND INSERTING INTO Bronze.brz_crm_sales_details'
			TRUNCATE TABLE Bronze.brz_crm_sales_details; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_crm_sales_details
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
			PRINT '--------'
		--------------


			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING TABLE AND INSERTING INTO Bronze.brz_crm_prd_info'
			TRUNCATE TABLE Bronze.brz_crm_prd_info; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_crm_prd_info
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		
		-------------------------------------------------
			PRINT '-------------------------------------------------------------------'
			PRINT 'Loading erp data'
			PRINT '-------------------------------------------------------------------'

		-----------


			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING AND INSERTING INTO TABLE Bronze.brz_erp_cust_az12'
			TRUNCATE TABLE Bronze.brz_erp_cust_az12; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_erp_cust_az12
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
			PRINT '--------'
		-----------
	

			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING AND INSERTING INTO TABLE Bronze.brz_erp_loc_a101'
			TRUNCATE TABLE Bronze.brz_erp_loc_a101; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_erp_loc_a101
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
			PRINT '--------'
		-----------
	
	    
			SET @start_time = GETDATE();
			PRINT '>>> TRUNCATING TABLE AND INSERTING INTO Bronze.brz_erp_px_cat_g1v2'
			TRUNCATE TABLE Bronze.brz_erp_px_cat_g1v2; -- must to avoid duplication of data

			BULK INSERT Bronze.brz_erp_px_cat_g1v2
			FROM 'C:\Users\TEJAS\Desktop\DWH-Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT'>> LOAD DURATION ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
			PRINT '--------'
		END TRY
		BEGIN CATCH
		PRINT'========================================'
		PRINT'ERROR OCCURED DURING BRONZE LOADING'
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE()
		PRINT'ERROR NUMBER' + CAST(ERROR_NUMBER() AS VARCHAR)
		PRINT'ERROR STATUS' + CAST(ERROR_STATE() AS VARCHAR)
		PRINT'========================================'
		END CATCH
		SET @batch_end_time = GETDATE();
		PRINT'>> COMPLETE BATCH LOAD DURATION ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds'
	    PRINT '--------'
END
