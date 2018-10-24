/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                  ***/
/***/

/*** OPTIONAL EXERCISE, shall only be looked at after Exercise 4 has been completed already                                  ***/
/*** Step 2B - Predictive Model Training - Train competitive Gradient Boosting Decision Tree Model *****************************/
/*** Input from Step1  ***/
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE;
--  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE;

/*******************************************************************************************************************************/
/*** CODE-STEP 28 ***/
/***  2B.1 - We train a Gradient Boosting Decision Tree model using the train-partition                             ***/
/*** STEP: Prepare algorithm parameter table; ***/
--Prepare algorithm parameter table;
--SET SCHEMA MLLAB_###;
--DROP TABLE #PAL_PARAMETER_TBL;
/*CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);*/

Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES('FOLD_NUM',       5,    NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES('CV_METRIC',      NULL, NULL, 'ERROR_RATE');
INSERT INTO #PAL_PARAMETER_TBL VALUES('REF_METRIC',     NULL, NULL, 'AUC');
INSERT INTO #PAL_PARAMETER_TBL VALUES('MAX_TREE_DEPTH', 6,    NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES('RANGE_ITER_NUM', NULL, NULL, '[10,3,20]');
INSERT INTO #PAL_PARAMETER_TBL VALUES('RANGE_LEARNING_RATE',  NULL, NULL, '[0.1,10,1.0]');
INSERT INTO #PAL_PARAMETER_TBL VALUES('RANGE_MIN_SPLIT_LOSS', NULL, NULL, '[0.0,10,1.0]');
INSERT INTO #PAL_PARAMETER_TBL VALUES ('HAS_ID',1,NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('DEPENDENT_VARIABLE',NULL,NULL,'ContractActivityLABEL');
INSERT INTO #PAL_PARAMETER_TBL VALUES ('ROW_SAMPLE_RATE',NULL,0.7,NULL);

/*** CODE-STEP 29 ***/
/*** STEP: Prepare the table to save the model to; ***/
--DROP TABLE PAL_GBDT_MODEL_TBL; -- for predict followed
CREATE COLUMN TABLE PAL_GBDT_MODEL_TBL ( 
	"ROW_INDEX" INTEGER,
	"KEY" NVARCHAR(1000),
	"VALUE" NVARCHAR(1000)
);
truncate table PAL_GBDT_MODEL_TBL;

/*** STEP: Prepare further statistical output tables, for later result inspection, independent of the algorithm call              ***/
DROP TABLE #P4;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P4 /*VARIABLE_IMPORTANCE*/ (
	"VARIABLE_NAME" NVARCHAR(40),
	"IMPORTANCE" DOUBLE
);
drop table #P5;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P5 /*CONFUSION_MATRIX (for classification only)*/ (
	"ACTUAL_CLASS" NVARCHAR(100),
	"PREDICTED_CLASS" NVARCHAR(100),
	"COUNT" Double 
);

drop table #P6;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P6 /*STATISTICS table*/ (
	"STAT_NAME" NVARCHAR(1000),
	"STAT_VALUE" NVARCHAR(1000) 
);

--drop table #P7;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P7 /*CROSS_VALIDATION*/ (
	"PARM_NAME" NVARCHAR(256),
	"INT_VALUE" INTEGER, 
	"DOUBLE_VALUE" DOUBLE,
	"STRING_VALUE" NVARCHAR(1000)
);


/*** CODE-STEP 30 ***/
 /*** STEP: Run the Gradient Boosting Decision Tree procedure and inspect the model and algorithm outputs ***/
CALL _SYS_AFL.PAL_GBDT(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE, #PAL_PARAMETER_TBL, PAL_GBDT_MODEL_TBL, #P4,#P5, #P6, #P7) WITH OVERVIEW;

select * from #P4; --VAR IMPORTANCE;
select * from #P5; --confusion matrix for the traindata;
select * from PAL_GBDT_MODEL_TBL;


/*** CODE-STEP 31 ***/
/*******************************************************************************************************************************/
/***   2B.2 - We validate the trained model by predicting against the train-partition and the validate-                      ***/
/***          partition, then compared predicted versus original churn behavior and further qualiy                           ***/
/***          the model quality using statistics like the confustion matrix, area under curve, etc ...                       ***/

/*** STEP: For predicting with the Train- and Validate-Partition, we need to create input views without                      ***/
/***       the observed churn behaviour column ContractActivityLABEL                                                         ***/
/*** Input from Step 2A.2 -  ***/
-- resuse Training-Sample subset/partition: MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED
select count(*) from  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED ;
--select top 10 * from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED ;

-- resuse Validation-Sample subset/partition: MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE
select count(*) from  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE_PRED ;


/*** STEP: Prepare the Parameter Table for the Prediction-procedure call with the trained model ***/
truncate table #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL); --value=1  doubles the records;

/*** IMPORTANT NOTE: The GBDT result outputs the confidence in string format, therefore the result table structure is different ***/
drop table #RESULT;
CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" NVARCHAR(100)
);

/*** CODE-STEP 32 ***/
/*** STEP: Predict with Gradient Boosting Decision Tree - Predict for Train-partition and save results ***/
--truncate table #RESULT;
CALL _SYS_AFL.PAL_GBDT_PREDICT(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED, PAL_GBDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;

Select count (distinct "SCORE") from #RESULT;
select count (*) from #RESULT;
Select count (*) as NCount, "SCORE" from #RESULT group by "SCORE"; 

Select count (*) as NCount, "SCORE" from #RESULT where "SCORE"='Churned' AND (TO_DOUBLE("CONFIDENCE"))>0.5
group by "SCORE"; 
Select "ID" , 	"SCORE" ,	"CONFIDENCE" from #RESULT where "SCORE"='Churned' AND (TO_DOUBLE("CONFIDENCE"))>0.5 ;

--drop table #GBDT_TRAINSAMPLE_PRED;
CREATE LOCAL TEMPORARY COLUMN TABLE #GBDT_TRAINSAMPLE_PRED like  #RESULT with data;
select * from #GBDT_TRAINSAMPLE_PRED;
Select count (*) as NCount, "SCORE" from #GBDT_TRAINSAMPLE_PRED group by "SCORE";

/*** CODE-STEP 33 ***/
truncate table #RESULT;
CALL _SYS_AFL.PAL_GBDT_PREDICT(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE_PRED, PAL_GBDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;
Select count (distinct "SCORE") from #RESULT;
Select * from #RESULT;
Select count (*) as NCount, "SCORE" from #RESULT group by "SCORE"; --7 churned;
Select count (*) as NCount from #RESULT where "SCORE"='Churned';
select * from #RESULT order by "SCORE", "CONFIDENCE" DESC;
Select * from #RESULT where "SCORE"='Churned' order by "CONFIDENCE" DESC;


Select * from #RESULT where "SCORE"='Churned' order by "ID";


--drop table  #GBDT_VALSAMPLE_PRED;
CREATE LOCAL TEMPORARY COLUMN TABLE #GBDT_VALSAMPLE_PRED like  #RESULT with data;
select * from #GBDT_VALSAMPLE_PRED;
Select count (*) as NCount, "SCORE" from #GBDT_VALSAMPLE_PRED group by "SCORE"; --7 churned;


/*******************************************************************************************************************************/
/*** CODE-STEP 34 ***/
/*** STEP: Validate model quality by comparing the confusion matrix for trained- versus validate-partition resuls           ***/

/*** STEP: Create Input data structure for the PAL Confusion Matrix algorithm, combinig observed vs original behaviour       ***/

--drop table #GBDT_TRAINSAMPLE_CF_IN;
create LOCAL TEMPORARY column table #GBDT_TRAINSAMPLE_CF_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"PREDICTED_LABEL" NVARCHAR(100));
	Insert into MLLAB_###.#GBDT_TRAINSAMPLE_CF_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", P."SCORE" as "PREDICTED_LABEL"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE as O ,  #GBDT_TRAINSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #GBDT_TRAINSAMPLE_CF_IN;

--drop table #GBDT_VALSAMPLE_CF_IN;
create LOCAL TEMPORARY column table #GBDT_VALSAMPLE_CF_IN like #GBDT_TRAINSAMPLE_CF_IN ;
	Insert into #GBDT_VALSAMPLE_CF_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", P."SCORE" as "PREDICTED_LABEL"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE as O , #GBDT_VALSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #GBDT_VALSAMPLE_CF_IN;


/*** CODE-STEP 35 ***/		
/*** STEP: Prepare the Confusion Matrix algorithm parameter table **/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('BETA', NULL, 1, NULL);


/*** STEP: Run Confusion Matrix algorithm **/
CALL _SYS_AFL.PAL_CONFUSION_MATRIX(#GBDT_TRAINSAMPLE_CF_IN,#PAL_PARAMETER_TBL,?,?);

CALL _SYS_AFL.PAL_CONFUSION_MATRIX(#GBDT_VALSAMPLE_CF_IN,#PAL_PARAMETER_TBL,?,?);


/*** CODE-STEP 36 ***/	
/*******************************************************************************************************************************/
/*** STEP: Validate model quality by comparing the area under curve (AUC) for trained- versus validate-partition resuls      ***/

/*** STEP: Create Input data structure for the PAL AUC procedure, combinig observed vs original behaviour       ***/
/***       For AUC calculation, the original label and the probability for the predicted positive label is required as input ***/
-- input ID, Original label, prob of positive label
-- Note, now CONFIDENCE needs to be transformed to a DOUBLE-format column;

--drop table  #TRAINSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #GBDT_TRAINSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #GBDT_TRAINSAMPLE_AUC_IN;
	Insert into #GBDT_TRAINSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then TO_DOUBLE(P."CONFIDENCE") 
 		      ELSE 1-TO_DOUBLE(P."CONFIDENCE")  END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE as O , #GBDT_TRAINSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #GBDT_TRAINSAMPLE_AUC_IN;

--drop table  #VALSAMPLE_AUC_IN;
--drop table  #GBDT_VALSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #GBDT_VALSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #GBDT_VALSAMPLE_AUC_IN;
	Insert into #GBDT_VALSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then TO_DOUBLE(P."CONFIDENCE")  
 		      ELSE 1-TO_DOUBLE(P."CONFIDENCE")  END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE as O , #GBDT_VALSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #GBDT_VALSAMPLE_AUC_IN;

		
/*** CODE-STEP 37 ***/
/*** STEP: Prepare the AUC procedure parameter table **/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('POSITIVE_LABEL', NULL, NULL, 'Churned');

/*** STEP: Run AUC procedure, output is the AUC value and ROC data  **/
CALL _SYS_AFL.PAL_AUC( #GBDT_TRAINSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);
CALL _SYS_AFL.PAL_AUC( #GBDT_VALSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);

--SAVE ROC DATA
--drop table TRAINSAMPLE_ROC_data;
--create column table TRAINSAMPLE_ROC_data ("ID" INTEGER, "FPR" DOUBLE, "TPR" DOUBLE);
--CALL _SYS_AFL.PAL_AUC( #GBDT_TRAINSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,TRAINSAMPLE_ROC_data) with overview;

--drop table VALSAMPLE_ROC_data;
--create column table VALSAMPLE_ROC_data ("ID" INTEGER, "FPR" DOUBLE, "TPR" DOUBLE);
--CALL _SYS_AFL.PAL_AUC( #GBDT_VALSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,VALSAMPLE_ROC_data) with overview;


/*******************************************************************************************************************************/
/*** CODE-STEP 38 ***/
/*** 2B.3 - Once happy with the trained model, we save it to a permanent model-repository table                                ***/

-- Note the GBDT model table structure;
/*
DROP TABLE PAL_GBDT_MODEL_TBL; 
CREATE COLUMN TABLE PAL_GBDT_MODEL_TBL ( 
	"ROW_INDEX" INTEGER,
	"KEY" NVARCHAR(1000),
	"VALUE" NVARCHAR(1000)
);*/

/*** Input from STEP 2A.3 the the MODELREPOSITORY table ***/
--drop table "MODELREPOSITORY";
/*
CREATE column table "MODELREPOSITORY" (
 "MODEL_NAME" NVARCHAR(100),
 "MODEL_VERSION" DOUBLE,
 "MODEL_TYPE" NVARCHAR(100),
 "ROW_INDEX" INTEGER,
 "TREE_INDEX" INTEGER,
 "MODEL_CONTENT" NVARCHAR(5000),
 "KEY" NVARCHAR(1000),
 "VALUE" NVARCHAR(1000),
 "MODEL_DATE"  DATE DEFAULT CURRENT_DATE,
 "MODEL_TIME" TIME DEFAULT CURRENT_TIME
  )  UNLOAD PRIORITY 5 AUTO MERGE
;*/

-- SAVE GBDT MODEL;
 INSERT INTO "MODELREPOSITORY" select 'CUSTOMER_CHURN','1.2', 'GBDT', GBDT."ROW_INDEX", NULL, NULL, GBDT."KEY", GBDT."VALUE", 
              current_date, current_time 
         from PAL_GBDT_MODEL_TBL as GBDT;
 select * from "MODELREPOSITORY" WHERE  "MODEL_TYPE"='GBDT';


 /*** Results: ***/
--- 2B.3   SAVED GBDT Model in MLLAB_###."MODELREPOSITORY";










