/*** 	Code step 47  ***/
/*** 	Create a view on the input data and explore the data ***/

--Drop view WHOLESALE_DATA_TBL_VIEW;
Create view WHOLESALE_DATA_TBL_VIEW as
Select * from "MLLAB".WHOLESALE_CUSTOMER_TXNS;

Select * from WHOLESALE_DATA_TBL_VIEW;


/*** 	Code step 48  ***/
/*** 	Take the input data and and split input data into training and test set. Create the parameter table first ***/
--DROP TABLE #WHOLESALE_PARAMETER_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #WHOLESALE_PARAMETER_TBL ("PARAM_NAME" VARCHAR (256),"INT_VALUE" INTEGER,"DOUBLE_VALUE" DOUBLE,"STRING_VALUE" VARCHAR (1000));

INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('PARTITION_METHOD',1,null,null); --stratified sample
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('RANDOM_SEED',23,null,null); -- to ensure repeatability
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('HAS_ID',1,NULL,NULL); -- input has ID column
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('STRATIFIED_COLUMN',null,null,'CUSTOMERTYPE'); --to identify the column on which stratification should be based
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('TRAINING_PERCENT', null,0.7,null); --70% training data
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('TESTING_PERCENT', null,0.3,null); --30% test data
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('VALIDATION_PERCENT', null,0.0,null);


/*** 	Code step 49  ***/
/*** 	Create partition table ***/

--DROP TABLE WHOLESALE_PARTITION_TBL;
CREATE  COLUMN TABLE WHOLESALE_PARTITION_TBL (
    "IDCOL" INTEGER,
    "PARTITION_NUMBER" INTEGER
);

/*** 	Code step 50  ***/
/*** 	Create the actual partitions and store in table **/

CALL "_SYS_AFL"."PAL_PARTITION"(WHOLESALE_DATA_TBL_VIEW, #WHOLESALE_PARAMETER_TBL, WHOLESALE_PARTITION_TBL) with overview ;

/*** 	Code step 51  ***/
/*** 	Create the training view that contains input data set belonging to the training partition (#1) **/

--Drop View WHOLESALE_DATA_TBL_TRAININGSAMPLEVIEW;
Create View WHOLESALE_DATA_TBL_TRAININGSAMPLEVIEW
as select C.* from WHOLESALE_DATA_TBL_VIEW as C, WHOLESALE_PARTITION_TBL as P
where C."IDCOL"=P."IDCOL" and P."PARTITION_NUMBER"=1;

/*** 	Code step 52  ***/
/*** 	Display the training data **/
SELECT * from WHOLESALE_DATA_TBL_TRAININGSAMPLEVIEW;

/*** 	Code step 53  ***/
/*** 	We will initially create a decision tree for prediction, and see how it performs. We will then improve the model in the subsequent steps ***/
/***	Cross validation will be used here ***/
/*** 	The first step in creating a decision tree is to create the parameter table ***/

TRUNCATE TABLE #WHOLESALE_PARAMETER_TBL; 

INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('ALGORITHM', 3, NULL, NULL); -- CART 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MODEL_FORMAT', 1, NULL, NULL); -- JSON 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MIN_RECORDS_OF_PARENT', 4, NULL, NULL); -- min records in parent
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MIN_RECORDS_OF_LEAF', 3, NULL, NULL); -- min records in leaf
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('IS_OUTPUT_RULES', 1, NULL, NULL); --display rules
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('IS_OUTPUT_CONFUSION_MATRIX', 1, NULL, NULL); --display confusion matrix
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('CATEGORICAL_VARIABLE', NULL, NULL, 'CUSTOMERTYPE'); --identify the categorical variable
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.4, NULL); --thread resources to use
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('HAS_ID', 1, NULL, NULL); -- identify ID column
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('DEPENDENT_VARIABLE', NULL, NULL, 'CUSTOMERTYPE'); -- dependent variable name
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('RESAMPLING_METHOD', NULL, NULL, 'stratified_cv'); -- sampling method
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('EVALUATION_METRIC', NULL, NULL, 'AUC'); --which metric to display for evaluation
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('FOLD_NUM', 10, NULL, NULL); --how many fold cross validation
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('SEED', 25, NULL, NULL); --to ensure repeatability

/*** 	Code step 54  ***/
/*** 	Train the model, passing the parameter table. Note the AUC number and confusion matrix displayed  ***/

--DROP TABLE WHOLESALE_MODEL_TBL_DT1;  
CREATE COLUMN TABLE WHOLESALE_MODEL_TBL_DT1 (    "ROW_INDEX" INTEGER,    "MODEL_CONTENT" NVARCHAR(5000) ); --model table

--DROP TABLE #RULES_1;
CREATE LOCAL TEMPORARY COLUMN TABLE #RULES_1 (    "ROW_INDEX" INTEGER,    "RULES_CONTENT" NVARCHAR(5000) ); --rules table

--DROP TABLE #CONF_MATRIX_1;
CREATE LOCAL TEMPORARY COLUMN TABLE #CONF_MATRIX_1 (    "ACTUAL_CLASS" NVARCHAR(1000), "PREDICTED_CLASS" NVARCHAR(1000), "COUNT" INTEGER); --confusion matrix

--DROP TABLE #STATS_1;
CREATE LOCAL TEMPORARY COLUMN TABLE #STATS_1 (    "STAT_NAME" NVARCHAR(1000), "STAT_VALUE" NVARCHAR(1000)); --statistics

--DROP TABLE #CROSS_VAL_1;
CREATE LOCAL TEMPORARY COLUMN TABLE #CROSS_VAL_1 ("PARAM_NAME" VARCHAR (256),"INT_VALUE" INTEGER,"DOUBLE_VALUE" DOUBLE,"STRING_VALUE" NVARCHAR (1000)); --cross validation table

CALL _SYS_AFL.PAL_DECISION_TREE (WHOLESALE_DATA_TBL_TRAININGSAMPLEVIEW, #WHOLESALE_PARAMETER_TBL, WHOLESALE_MODEL_TBL_DT1, #RULES_1,#CONF_MATRIX_1, #STATS_1,#CROSS_VAL_1) with overview;

Select * FROM WHOLESALE_MODEL_TBL_DT1; -- model for decision tree
Select * FROM #STATS_1; -- note the AUC number
Select * FROM #CONF_MATRIX_1; -- note the confusion matrix

/*** 	Code step 55  ***/
/***	The next step is to improve the model above. We will do this by trying different combinations of parameters using the Hyper-parameter and use cross validation.  ***/ 
/*** 	Start by populating the parameter table  ***/
TRUNCATE TABLE #WHOLESALE_PARAMETER_TBL; 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('ALGORITHM', 3, NULL, NULL); -- CART 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MODEL_FORMAT', 1, NULL, NULL); -- JSON 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MIN_RECORDS_OF_PARENT_VALUES', NULL, NULL, '{3,4,5,6}'); --records in parent
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('MIN_RECORDS_OF_LEAF_VALUES', NULL, NULL, '{2,3,4,5,6,7,8}'); --various values of leaf node to try
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('IS_OUTPUT_RULES', 1, NULL, NULL); --output rules
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('IS_OUTPUT_CONFUSION_MATRIX', 1, NULL, NULL); --output confusion matrix
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('CATEGORICAL_VARIABLE', NULL, NULL, 'CUSTOMERTYPE'); --identify categorical variable
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.4, NULL); --thread resources to use
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('HAS_ID', 1, NULL, NULL); --identify data has ID column
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('DEPENDENT_VARIABLE', NULL, NULL, 'CUSTOMERTYPE'); --identify dependent variable
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('RESAMPLING_METHOD', NULL, NULL, 'stratified_cv'); --specify sampling method
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('EVALUATION_METRIC', NULL, NULL, 'AUC'); --specify what criteria to maximize
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('PARAM_SEARCH_STRATEGY', NULL, NULL, 'grid'); --parameter search technique
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('FOLD_NUM', 15, NULL, NULL); --number of folds in cross validation
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('SPLIT_THRESHOLD_VALUES', NULL, NULL, '{1e-1, 1e-2, 1e-3,1e-4,1e-5,1e-6,1e-7,1e-10}'); --different threshold values to try 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('PROGRESS_INDICATOR_ID', NULL, NULL, 'PAL_CROSS_VAL'); --ID for progress indicator
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('SEED', 240, NULL, NULL); --to ensure repeatability

/*** 	Code step 56  ***/
/*** 	Train the model  ***/
/*** 	As hyper-parameter technique is expected to result in better AUC, we will use this model ***/
--DROP TABLE WHOLESALE_MODEL_TBL_DT2;
CREATE COLUMN TABLE WHOLESALE_MODEL_TBL_DT2 (    "ROW_INDEX" INTEGER,    "MODEL_CONTENT" NVARCHAR(5000) ); 

--DROP TABLE #RULES_2;
CREATE LOCAL TEMPORARY COLUMN TABLE #RULES_2 (    "ROW_INDEX" INTEGER,    "RULES_CONTENT" NVARCHAR(5000) ); --rules table

--DROP TABLE #CONF_MATRIX_2;
CREATE LOCAL TEMPORARY COLUMN TABLE #CONF_MATRIX_2 (    "ACTUAL_CLASS" NVARCHAR(1000), "PREDICTED_CLASS" NVARCHAR(1000), "COUNT" INTEGER); --confusion matrix

--DROP TABLE #STATS_2;
CREATE LOCAL TEMPORARY COLUMN TABLE #STATS_2 (    "STAT_NAME" NVARCHAR(1000), "STAT_VALUE" NVARCHAR(1000)); --statistics

--DROP TABLE #CROSS_VAL_2;
CREATE LOCAL TEMPORARY COLUMN TABLE #CROSS_VAL_2 ("PARAM_NAME" VARCHAR (256),"INT_VALUE" INTEGER,"DOUBLE_VALUE" DOUBLE,"STRING_VALUE" NVARCHAR (1000)); --cross validation table

CALL _SYS_AFL.PAL_DECISION_TREE (WHOLESALE_DATA_TBL_TRAININGSAMPLEVIEW, #WHOLESALE_PARAMETER_TBL, WHOLESALE_MODEL_TBL_DT2, #RULES_2,#CONF_MATRIX_2, #STATS_2,#CROSS_VAL_2) with overview;

Select * FROM #STATS_2; -- note the AUC number
Select * FROM #CONF_MATRIX_2; -- note the confusion matrix
Select * FROM #CROSS_VAL_2; -- note the values selected for min_records_of_parent, min_records_of_leaf, etc


/*** 	Code step 57  ***/
/*** 	Create the test view needed to test the model ***/
--Drop view WHOLESALE_DATA_TBL_TESTSAMPLEVIEW;
Create View WHOLESALE_DATA_TBL_TESTSAMPLEVIEW
as select C."IDCOL", C."FRESHPRODUCE",C."MILK",C."GROCERY",C."FROZEN", C."DETERGENTS",C."MEATDELI" from WHOLESALE_DATA_TBL_VIEW as C, WHOLESALE_PARTITION_TBL as P
where C."IDCOL"=P."IDCOL" and P."PARTITION_NUMBER"=2;

/*** 	Code step 58  ***/
/*** 	View the test data  ***/
select * from WHOLESALE_DATA_TBL_TESTSAMPLEVIEW;

/*** 	Code step 59  ***/
/*** 	Create predicted result table needed for storing the predictions  ***/
--DROP TABLE WHOLESALE_PRED_TBL;  

CREATE COLUMN TABLE WHOLESALE_PRED_TBL ("IDCOL" INTEGER, "SCORE" NVARCHAR(100), "CONFIDENCE" DOUBLE ); 

/*** 	Code step 60  ***/
/*** 	Score against test data  ***/
TRUNCATE TABLE #WHOLESALE_PARAMETER_TBL; 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL); 
INSERT INTO #WHOLESALE_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL); 
CALL _SYS_AFL.PAL_DECISION_TREE_PREDICT (WHOLESALE_DATA_TBL_TESTSAMPLEVIEW, WHOLESALE_MODEL_TBL_DT2, #WHOLESALE_PARAMETER_TBL, WHOLESALE_PRED_TBL) with overview;

/*** 	Code step 61  ***/
/*** 	View the predicted results  ***/
Select * from WHOLESALE_PRED_TBL;

/*** 	Code step 62  ***/
/*** 	Compare predicted vs actual results  ***/
--Drop view WHOLESALE_COMPARE_RESULTS;
Create view WHOLESALE_COMPARE_RESULTS
as select C."IDCOL", C."SCORE",C."CONFIDENCE",P."CUSTOMERTYPE" from WHOLESALE_PRED_TBL as C, WHOLESALE_DATA_TBL_VIEW as P
where C."IDCOL"=P."IDCOL";

/*** visualize the data and compute the cases where the prediction does not match actuals, using the test data  ***/
select * from WHOLESALE_COMPARE_RESULTS;
select count(*) from WHOLESALE_COMPARE_RESULTS;
select count(*) from WHOLESALE_COMPARE_RESULTS as R
where R.SCORE <> R.CUSTOMERTYPE;