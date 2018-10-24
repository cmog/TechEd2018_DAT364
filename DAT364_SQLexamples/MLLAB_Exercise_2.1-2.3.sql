/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                  ***/
/***/
/*** Step 2A - Predictive Model Training ****************************************************************************************/
/*** Exercise objective: We want to train a classification model to predict customer churn using the Random Decision Tree     ***/
/***                     algorithm.                                                                                          ***/
/***                     2A.1 - We train a Random Decision Tree model using the train-partition                             ***/
/***                     2A.2 - We validate the trained model by predicting against the train-partition and the validate-    ***/
/***                            partition, then compare predicted versus original churn behavior and further qualify         ***/
/***                            the model quality using statistics like the confusion matrix, area under curve, etc ...     ***/
/***                     2A.3 - Once happy with the trained model, we save it to a permanent model repository table            ***/
--- Note: Random Decision Trees also known as Random Forests are one of the most powerful, fully automated, machine learning techniques. With very little data preparation or modeling expertise, ;
---          analysts can effortlessly obtain surprisingly effective models. Random Decision Trees are an essential component in the modern data scientist’s toolkit..;
---          Further Random Decision Trees  is a so-called ensemble model algorithm, which runs a series of classification or regression models over random (bootstrap samples) from the data ;
---          it combines and fits those results by voting (classification) or averaging (regression), resulting in robust and high prediction quality models.;

/*** Input from Step1  ***/
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE;
--  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE;

/*** CODE-STEP 05 ***/
/*******************************************************************************************************************************/
/***                     2A.1 - We train a Random Decision Tree model using the train-partition                             ***/
/*** STEP: Prepare algorithm parameter table; ***/
--SET SCHEMA MLLAB_###;
--DROP TABLE #PAL_PARAMETER_TBL;
/*CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);*/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('TREES_NUM', 100, NULL, NULL); --number of parallel competing decision trees modeled internally within the algorithm;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('TRY_NUM', 5, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('SEED', 2, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('SPLIT_THRESHOLD', NULL, 1e-5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('CALCULATE_OOB', 1, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('NODE_SIZE', 1, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 1.0, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('HAS_ID',1,NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('DEPENDENT_VARIABLE',NULL,NULL,'ContractActivityLABEL'); -- target column, for which we want to train a classification prediction model;

/*** STEP: Prepare the table to save the model to; ***/
--DROP TABLE PAL_RDT_MODEL_TBL;  
CREATE COLUMN TABLE PAL_RDT_MODEL_TBL (
	"ROW_INDEX" INTEGER,
	"TREE_INDEX" INTEGER,
	"MODEL_CONTENT" NVARCHAR(5000)
);
--truncate table PAL_RDT_MODEL_TBL;

/*** CODE-STEP 06 ***/
/*** STEP: Prepare further statistical output tables, for later result inspection, independent of the algorithm call              ***/
/*** You can run the algorithm with "?" for the output table types, explicitly specifying tables allows to later query the result ***/
--DROP TABLE #P4;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P4 /*VARIABLE_IMPORTANCE*/ (
	"COLUMN_NAME" NVARCHAR(40),
	"IMPORTANCE" DOUBLE
);
--drop table #P5;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P5 /*OUT_OF_BAG_ERROR*/ (
	"TREE_INDEX" INTEGER,
	"OOB_ERROR" DOUBLE /*Out-of-bag error rate or mean squared error for random decision trees up to indexed tree */
);
--drop table #P6;
CREATE LOCAL TEMPORARY  COLUMN TABLE #P6 /*CONFUSION_MATRIX (for classification only)*/ (
	"ACTUAL_CLASS" NVARCHAR(100),
	"PREDICTED_CLASS" NVARCHAR(100),
	"COUNT" Double 
);

/*** CODE-STEP 07 ***/
 /*** STEP: Run the Random Decision Tree algorithm and inspect the model ***/
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES (MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE, #PAL_PARAMETER_TBL, PAL_RDT_MODEL_TBL, #P4, #P5, #P6) with overview;
select * from #P4; -- inspect the variable importance statistics;
--select * from #P6; --confusion matrix for the traindata;
--select * from PAL_RDT_MODEL_TBL;
select "ROW_INDEX", "TREE_INDEX", CAST("MODEL_CONTENT" as VARCHAR(1000)) from "PAL_RDT_MODEL_TBL";

/*** CODE-STEP 08 ***/
/*******************************************************************************************************************************/
/***   2A.2 - We validate the trained model by predicting against the train-partition and the validate-                      ***/
/***          partition, then compare predicted versus original churn behavior and further qualify                           ***/
/***          the model quality using statistics like the confustion matrix, area under curve, etc ...                       ***/

/*** STEP: For predicting with the Train- and Validate-Partition, we need to create input views without                      ***/
/***       the observed churn behaviour column ContractActivityLABEL                                                         ***/
-- create view like TRAINSAMPLE, but without the LABEL-column ;
Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED
as select C."AccountID",	C."ServiceType" ,	 C."ServiceName",	 C."DataAllowance_MB",	 C."VoiceAllowance_Minutes" ,	 C."SMSAllowance_N_Messages" ,
	 C."DataUsage_PCT" ,	 C."DataUsage_PCT_PM",	  C."DataUsage_PCT_PPM" ,
	 C."VoiceUsage_PCT" , 	 C."VoiceUsage_PCT_PM" , 	 C."VoiceUsage_PCT_PPM" ,
	 C."SMSUsage_PCT" , 	 C."SMSUsage_PCT_PM" , 	 C."SMSUsage_PCT_PPM" ,
	 C."Revenue_Month" ,	  C."Revenue_Month_PM",	   C."Revenue_Month_PPM" ,	    C."Revenue_Month_PPPM" ,
	 C."ServiceFailureRate_PCT" ,	 C."ServiceFailureRate_PCT_PM" ,	 C."ServiceFailureRate_PCT_PPM" ,
	 C."CustomerLifetimeValue_USD" ,	  C."CustomerLifetimeValue_USD_PM" ,	   C."CustomerLifetimeValue_USD_PPM" ,
	 C."Device_Lifetime" ,	 	 C."Device_Lifetime_PM" ,	 	 	 C."Device_Lifetime_PPM" 	 
from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=1;
select count(*) from  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED ;

Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE_PRED
as select C."AccountID",	C."ServiceType" ,	 C."ServiceName",	 C."DataAllowance_MB",	 C."VoiceAllowance_Minutes" ,	 C."SMSAllowance_N_Messages" ,
	 C."DataUsage_PCT" ,	 C."DataUsage_PCT_PM",	  C."DataUsage_PCT_PPM" ,
	 C."VoiceUsage_PCT" , 	 C."VoiceUsage_PCT_PM" , 	 C."VoiceUsage_PCT_PPM" ,
	 C."SMSUsage_PCT" , 	 C."SMSUsage_PCT_PM" , 	 C."SMSUsage_PCT_PPM" ,
	 C."Revenue_Month" ,	  C."Revenue_Month_PM",	   C."Revenue_Month_PPM" ,	    C."Revenue_Month_PPPM" ,
	 C."ServiceFailureRate_PCT" ,	 C."ServiceFailureRate_PCT_PM" ,	 C."ServiceFailureRate_PCT_PPM" ,
	 C."CustomerLifetimeValue_USD" ,	  C."CustomerLifetimeValue_USD_PM" ,	   C."CustomerLifetimeValue_USD_PPM" ,
	 C."Device_Lifetime" ,	 	 C."Device_Lifetime_PM" ,	 	 	 C."Device_Lifetime_PPM"  from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=3;
select count(*) from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE_PRED ;


/*** CODE-STEP 09 ***/
/*** STEP: Prepare the Parameter Table for the Prediction-procedure call with the trained model ***/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL);

--drop table #RESULT;
CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" Double
);
--truncate table #RESULT;

/*** CODE-STEP 10 ***/
/*** STEP: Predict with Random Decision Trees - Predict for Train-partition and save results ***/
truncate table #RESULT;
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED, PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;

Select count (distinct "SCORE") from #RESULT;
Select * from #RESULT;
Select count (*) as NCount, "SCORE" from #RESULT group by "SCORE"; 
Select count (*) as NCount from #RESULT where "SCORE"='Churned';
select * from #RESULT order by "SCORE", "CONFIDENCE" DESC;
Select * from #RESULT where "SCORE"='Churned' order by "CONFIDENCE" DESC;

CREATE LOCAL TEMPORARY COLUMN TABLE #TRAINSAMPLE_PRED like  #RESULT with data;
select * from #TRAINSAMPLE_PRED;

/*** CODE-STEP 11 ***/
/*** STEP: Predict with Random Decision Trees - Predict for Validation-partition and save results ***/
truncate table #RESULT;
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE_PRED, PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;
Select count (distinct "SCORE") from #RESULT;
Select * from #RESULT;
Select count (*) as NCount, "SCORE" from #RESULT group by "SCORE"; 
Select count (*) as NCount from #RESULT where "SCORE"='Churned';
select * from #RESULT order by "SCORE", "CONFIDENCE" DESC;
Select * from #RESULT where "SCORE"='Churned' order by "CONFIDENCE" DESC;

Select * from #RESULT where "SCORE"='Churned' order by "ID";

CREATE LOCAL TEMPORARY COLUMN TABLE #VALSAMPLE_PRED like  #RESULT with data;
select * from #VALSAMPLE_PRED;


/*** CODE-STEP 12 ***/
/*******************************************************************************************************************************/
/*** STEP: Validate model quality by comparing the confusion matrix for trained- versus validate-partition results           ***/

/*** STEP: Create Input data structure for the PAL Confusion Matrix algorithm, combinig observed vs original behaviour       ***/
--drop table  #TRAINSAMPLE_CF_IN;
create LOCAL TEMPORARY column table #TRAINSAMPLE_CF_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"PREDICTED_LABEL" NVARCHAR(100));
	Insert into #TRAINSAMPLE_CF_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", P."SCORE" as "PREDICTED_LABEL"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE as O , #TRAINSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #TRAINSAMPLE_CF_IN;

create LOCAL TEMPORARY column table #VALSAMPLE_CF_IN like #TRAINSAMPLE_CF_IN ;
	Insert into #VALSAMPLE_CF_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", P."SCORE" as "PREDICTED_LABEL"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE as O , #VALSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #VALSAMPLE_CF_IN;

/*** CODE-STEP 13 ***/		
/*** STEP: Prepare the Confusion Matrix algorithm parameter table **/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('BETA', NULL, 1, NULL);


/*** STEP: Run Confusion Matrix algorithm **/
CALL _SYS_AFL.PAL_CONFUSION_MATRIX(#TRAINSAMPLE_CF_IN,#PAL_PARAMETER_TBL,?,?);

CALL _SYS_AFL.PAL_CONFUSION_MATRIX(#VALSAMPLE_CF_IN,#PAL_PARAMETER_TBL,?,?);

-- Structure of the confusion matrix result table;
--drop table #CM_TRAIN;
/*CREATE LOCAL TEMPORARY COLUMN TABLE #CM_TRAIN (
	"ACTUAL_CLASS" NVARCHAR(100),
	"PREDICTED_CLASS" NVARCHAR(100),
	"COUNT" Double
); */

/*** CODE-STEP 14 ***/	
/*******************************************************************************************************************************/
/*** STEP: Validate model quality by comparing the area under curve (AUC) for trained- versus validate-partition resuls      ***/

/*** STEP: Create Input data structure for the PAL AUC procedure, combinig observed vs original behaviour       ***/
/***       For AUC calculation, the original label and the probability for the predicted positive label is required as input ***/
-- input ID, Original label, prob of positive label

--drop table  #TRAINSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #TRAINSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #TRAINSAMPLE_AUC_IN;
	Insert into #TRAINSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then P."CONFIDENCE" 
 		      ELSE 1-P."CONFIDENCE" END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE as O , #TRAINSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #TRAINSAMPLE_AUC_IN;

--drop table  #VALSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #VALSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #VALSAMPLE_AUC_IN;
	Insert into #VALSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then P."CONFIDENCE" 
 		      ELSE 1-P."CONFIDENCE" END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE as O , #VALSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #VALSAMPLE_AUC_IN;
 

/*** STEP: Prepare the AUC procedure parameter table **/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('POSITIVE_LABEL', NULL, NULL, 'Churned'); -- indicate which is the positive label value;

/*** STEP: Run AUC procedure, output is the AUC value and ROC data  **/
CALL _SYS_AFL.PAL_AUC( #TRAINSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);


CALL _SYS_AFL.PAL_AUC( #VALSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);

/*
--drop table TRAINSAMPLE_ROC_data;
create column table TRAINSAMPLE_ROC_data ("ID" INTEGER, "FPR" DOUBLE, "TPR" DOUBLE);
CALL _SYS_AFL.PAL_AUC( #TRAINSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,TRAINSAMPLE_ROC_data) with overview;

--drop table VALSAMPLE_ROC_data;
create column table VALSAMPLE_ROC_data ("ID" INTEGER, "FPR" DOUBLE, "TPR" DOUBLE);
CALL _SYS_AFL.PAL_AUC( #VALSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,VALSAMPLE_ROC_data) with overview;
*/

 /*** CODE-STEP 15 ***/	
/*******************************************************************************************************************************/
/*** 2A.3 - Once happy with the trained model, we save it to a permanent modelrepository table                                ***/
-- Note, structure of trained model table is different, therefore the structure for permanently storing different model types, needs to support saving different model structures ; 
/*
CREATE COLUMN TABLE PAL_RDT_MODEL_TBL (
	"ROW_INDEX" INTEGER,
	"TREE_INDEX" INTEGER,
	"MODEL_CONTENT" NVARCHAR(5000)
);
DROP TABLE PAL_GBDT_MODEL_TBL; -- for predict followed
CREATE COLUMN TABLE PAL_GBDT_MODEL_TBL ( 
	"ROW_INDEX" INTEGER,
	"KEY" NVARCHAR(1000),
	"VALUE" NVARCHAR(1000)
);*/

/*** STEP:  Create the MODELREPOSITORY table ***/
-- save model into model repository table
--drop table "MODELREPOSITORY";
CREATE column table "MODELREPOSITORY" (
 "MODEL_NAME" NVARCHAR(100),
 "MODEL_VERSION" DOUBLE,
 "MODEL_TYPE" NVARCHAR(100),
 "ROW_INDEX" INTEGER,
 "TREE_INDEX" INTEGER,
 "MODEL_CONTENT" NVARCHAR(5000),
 "KEY" NVARCHAR(1000),
 "VALUE" NVARCHAR(1000),
 "MODEL_DATE"   DATE DEFAULT CURRENT_DATE,
 "MODEL_TIME" TIME DEFAULT CURRENT_TIME
  )  UNLOAD PRIORITY 5 AUTO MERGE
;

/*** CODE-STEP 16 ***/	
/*** STEP:  Save the RDT model to the MODELREPOSITORY table ***/
--- SAVE Model in MLLAB_###."MODELREPOSITORY";
 -- SAVE RDT MODEL;
  INSERT INTO "MODELREPOSITORY" select 'CUSTOMER_CHURN','1.1', 'RDT', RDT."ROW_INDEX", RDT."TREE_INDEX", RDT."MODEL_CONTENT", NULL, NULL, 
              current_date, current_time 
         from PAL_RDT_MODEL_TBL as RDT;
		 
 --select * from "MODELREPOSITORY";
  select  "MODEL_NAME", "MODEL_VERSION", "MODEL_TYPE", "ROW_INDEX",  "TREE_INDEX", CAST("MODEL_CONTENT" as VARCHAR(1000)),  "MODEL_DATE" from "MODELREPOSITORY";
 
 -- SAVE GBDT MODEL;
 /*INSERT INTO "MODELREPOSITORY" select 'CUSTOMER_CHURN','1.1', 'GBDT', GBDT."ROW_INDEX", NULL, NULL, GBDT."KEY", GBDT."VALUE", 
              current_date, current_time 
         from PAL_GBDT_MODEL_TBL as GBDT;*/

 

/*** Results: ***/
--- 2A.3   SAVED RDT Model in MLLAB_###."MODELREPOSITORY";




