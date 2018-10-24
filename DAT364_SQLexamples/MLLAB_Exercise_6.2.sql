/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 4st August 2018                                                                 ***/


/*** OPTIONAL EXERCISE 6.2       **********************************************************************************************/
/*** Using PAL Segmented PAL Algorithm Invocation > here a segemented Forecasting model                                ********/

/*** CODE-STEP 68 ***/
/*** Prepare the Segmented Forecast ***/


/* Example 1: Group specific parameters

Both the data table and the parameter table have the group identifier column. Rows in the data table and the parameter table with the same group identifier are mapped and processed together. A set of result data will be generated for each group. The result from different groups of input data will be collected together to build the final result.*/
SET SCHEMA MLLAB_###;


SELECT * from MLLAB.RETAILSALES_BY_PRODUCT;

DROP TABLE #PAL_PARAMETER_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
   "PRODUCT_ID" NVARCHAR(10),
	"PARAM_NAME" NVARCHAR(100),
	"INT_VALUE" INTEGER, 
	"DOUBLE_VALUE" DOUBLE, 
	"STRING_VALUE" NVARCHAR(100)
);
truncate table #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'ADAPTIVE_METHOD',0, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'MEASURE_NAME', NULL, NULL, 'MSE');
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'ALPHA', NULL,0.1, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'DELTA', NULL,0.2, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'FORECAST_NUM',12, NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'EXPOST_FLAG',1, NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'PREDICTION_CONFIDENCE_1', NULL, 0.8, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_A', 'PREDICTION_CONFIDENCE_2', NULL, 0.95, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'ADAPTIVE_METHOD',0, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'MEASURE_NAME', NULL, NULL, 'MSE');
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'ALPHA', NULL,0.1, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'DELTA', NULL,0.2, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'FORECAST_NUM',12, NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'EXPOST_FLAG',1, NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'PREDICTION_CONFIDENCE_1', NULL, 0.75, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('Product_B', 'PREDICTION_CONFIDENCE_2', NULL, 0.8, NULL);

DROP TABLE RETAILSALES_FORECAST_BY_PRODUCT;
CREATE COLUMN TABLE RETAILSALES_FORECAST_BY_PRODUCT (
   "PRODUCT_ID" NVARCHAR(10),
    "DAY" INT,
    "SALES" DOUBLE,
    "80%_LOWER" DOUBLE,
    "80%_UPPER" DOUBLE,
    "95%_LOWER" DOUBLE,
    "95%_UPPER" DOUBLE
);

DROP TABLE RETAILSALES_FORECAST_BY_PRODUCT_STATS;
CREATE COLUMN TABLE RETAILSALES_FORECAST_BY_PRODUCT_STATS (
   "PRODUCT_ID" NVARCHAR(10),
    "STAT_NAME" NVARCHAR(100),
    "STAT_VALUE" DOUBLE
);


/*** CODE-STEP 69 ***/
/*** RUN the Segmented Forecast ***/

CALL _SYS_AFL.PAL_SINGLE_EXPSMOOTH(MLLAB.RETAILSALES_BY_PRODUCT, #PAL_PARAMETER_TBL, RETAILSALES_FORECAST_BY_PRODUCT, RETAILSALES_FORECAST_BY_PRODUCT_STATS)
    WITH OVERVIEW WITH HINT( PARALLEL_BY_PARAMETER_VALUES(p1.PRODUCT_ID, p2.PRODUCT_ID) );

SELECT * FROM RETAILSALES_FORECAST_BY_PRODUCT;
SELECT * FROM RETAILSALES_FORECAST_BY_PRODUCT_STATS;