/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                 ***/
/***/

/*** CODE-STEP 39 ***/
/*** OPTIONAL EXERCISE 5, shall only be looked at after Exercise 2B has been completed already                                    ***/
/*** Ex. 5.4 - Compare competitive models RDT and GBDT *****************************/
/***           While in Steps 2A.2 (Ex 2.2) and 2B.2 (Ex 5.3) we looked to validate the individual algorithm model performance by ***/
/***           comparing model quality statistics btw the train-partition and the validate-partitions                             ***/
/***           NOW, we want to compare model prediction quality statistics for different algorithm models (RDT and GBDT)          ***/
/***                against a neutral test-sample-partitions, which shall not have been used yet to optimize the models we seek   ***/
/***                to compare now                                                                                                ***/
/***                            the model quality using statistics like the confusion matrix, area under curve, etc ...           ***/
/***           Finally, the better competing model shall be selected as the champion model and saved to the                       ***/
/***           permanent modelrepository table                                                                                    ***/

/*** Input from Step1  ***/
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE;

/*** STEP: For predicting with the Train- and TEST-Partition, we need to create input views without                          ***/
/***       the observed churn behaviour column ContractActivityLABEL                                                         ***/
-- resuse Training-Sample subset/partition: MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED;
select count(*) from  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE_PRED ;

-- create the Test-Sample subset/partition;
Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE_PRED
as select C."AccountID",	C."ServiceType" ,	 C."ServiceName",	 C."DataAllowance_MB",	 C."VoiceAllowance_Minutes" ,	 C."SMSAllowance_N_Messages" ,
	 C."DataUsage_PCT" ,	 C."DataUsage_PCT_PM",	  C."DataUsage_PCT_PPM" ,
	 C."VoiceUsage_PCT" , 	 C."VoiceUsage_PCT_PM" , 	 C."VoiceUsage_PCT_PPM" ,
	 C."SMSUsage_PCT" , 	 C."SMSUsage_PCT_PM" , 	 C."SMSUsage_PCT_PPM" ,
	 C."Revenue_Month" ,	  C."Revenue_Month_PM",	   C."Revenue_Month_PPM" ,	    C."Revenue_Month_PPPM" ,
	 C."ServiceFailureRate_PCT" ,	 C."ServiceFailureRate_PCT_PM" ,	 C."ServiceFailureRate_PCT_PPM" ,
	 C."CustomerLifetimeValue_USD" ,	  C."CustomerLifetimeValue_USD_PM" ,	   C."CustomerLifetimeValue_USD_PPM" ,
	 C."Device_Lifetime" ,	 	 C."Device_Lifetime_PM" ,	 	 	 C."Device_Lifetime_PPM"  
	 from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=2;
select count(*) from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE_PRED ;


/*** CODE-STEP 40 ***/
/*** STEP: Prepare the Parameter Table for the Prediction-procedure call with the trained model ***/
--DROP TABLE #PAL_PARAMETER_TBL;
/*CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);*/

Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL); --value=1  doubles the records;

/*** CODE-STEP 41 ***/
/*** STEP: Predict with Gradient Boosting Decision Tree - Predict for TEST-partition and save results ***/
drop table #RESULT;
CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" NVARCHAR(100)
);
--truncate table #RESULT;

select * from PAL_GBDT_MODEL_TBL;


CALL _SYS_AFL.PAL_GBDT_PREDICT(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE, PAL_GBDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;

CREATE LOCAL TEMPORARY COLUMN TABLE #GBDT_TESTSAMPLE_PRED like  #RESULT with data;
select * from #GBDT_TESTSAMPLE_PRED;
Select count (*) as NCount, "SCORE" from #GBDT_TESTSAMPLE_PRED group by "SCORE";

/*** CODE-STEP 42 ***/
/*** STEP: Predict with Random Decision Tree - Predict for TEST-partition and save results ***/
--- Note, result table CONFIDENCE column here is DOUBLE data type;
drop table #RESULT;
CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" DOUBLE
);
--truncate table  #RESULT;
 
-- select * from PAL_RDT_MODEL_TBL;

CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE_PRED, PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;

Select * from #RESULT;
--drop table #TESTSAMPLE_PRED;
CREATE LOCAL TEMPORARY COLUMN TABLE #TESTSAMPLE_PRED like  #RESULT with data;
select * from #TESTSAMPLE_PRED;
Select count (*) as NCount, "SCORE" from #TESTSAMPLE_PRED group by "SCORE"; 

/*** CODE-STEP 43 ***/
/*******************************************************************************************************************************/
/*** STEP: Validate model quality by comparing the area under curve (AUC) for RDT- versus GBDT-partition results             ***/
/***       running against the TEST-partition data.                                                                          ***/

/*** STEP: Create Input data structure for the PAL AUC procedure, combinig observed vs original behaviour                    ***/
/***       For AUC calculation, the original label and the probability for the predicted positive label is required as input ***/

---AUC for GBDT
--drop table  #GBDT_TESTSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #GBDT_TESTSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #GBDT_TESTSAMPLE_AUC_IN;
	Insert into #GBDT_TESTSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then TO_DOUBLE(P."CONFIDENCE") 
 		      ELSE 1-TO_DOUBLE(P."CONFIDENCE")  END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE as O , #GBDT_TESTSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #GBDT_TESTSAMPLE_AUC_IN;

---AUC for Random DT
--drop table  #RDT_TESTSAMPLE_AUC_IN;
create LOCAL TEMPORARY column table #RDT_TESTSAMPLE_AUC_IN (
	"ID" INTEGER, "ORIGINAL_LABEL" NVARCHAR(100),
	"CHURN_PROBABILITY" DOUBLE);
truncate table #RDT_TESTSAMPLE_AUC_IN;
	Insert into #RDT_TESTSAMPLE_AUC_IN
 		select O."AccountID", O."ContractActivityLABEL" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = 'Churned' Then P."CONFIDENCE" 
 		      ELSE 1-P."CONFIDENCE" END 
 		      as "CHURN_PROBABILITY"
		from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE as O , #TESTSAMPLE_PRED as P
		where O."AccountID"=P."ID";
		Select * from #RDT_TESTSAMPLE_AUC_IN;

		
		
/*** CODE-STEP 44 ***/		
/*** STEP: Prepare the AUC procedure parameter table **/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('POSITIVE_LABEL', NULL, NULL, 'Churned');

/*** STEP: Run AUC procedure, output is the AUC value and ROC data  **/		
CALL _SYS_AFL.PAL_AUC( #GBDT_TESTSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);
--AUC with TESTSAMPLE-SET and GBDT:0.8539072563331904;

		
CALL _SYS_AFL.PAL_AUC( #RDT_TESTSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,?,?);
-- >> RDT AUC with testsample data partition: 0.9474023185916701;


/*** CODE-STEP 45 ***/	
/*******************************************************************************************************************************/
/*** STEP: Automate model selection over competing models, based on AUC statistics                                           ***/

/*** STEP: Save the AUC value to tables **/
-- based on calculated AUC, you could now automate the model selection.
CREATE COLUMN TABLE AUC_RDT /*STATISTICS table*/ (
	"STAT_NAME" NVARCHAR(100),
	"STAT_VALUE" DOUBLE 
);
CREATE COLUMN TABLE AUC_GBDT /*STATISTICS table*/ (
	"STAT_NAME" NVARCHAR(100),
	"STAT_VALUE" DOUBLE 
);
truncate table AUC_RDT;
truncate table AUC_GBDT;
CALL _SYS_AFL.PAL_AUC( #RDT_TESTSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,AUC_RDT,?) with overview;
CALL _SYS_AFL.PAL_AUC( #GBDT_TESTSAMPLE_AUC_IN,#PAL_PARAMETER_TBL,AUC_GBDT,?) with overview;
select * from AUC_RDT;


/*** CODE-STEP 46 ***/	
/*** STEP: Conditionally save model with higher AUC to model repository table ***/
do 
begin  
  DECLARE LSV_AUC_RDT DOUBLE;
  DECLARE LSV_AUC_GBDT DOUBLE;
    SELECT "STAT_VALUE" into LSV_AUC_RDT from AUC_RDT;
    SELECT "STAT_VALUE" into LSV_AUC_GBDT from AUC_GBDT;
    
    IF :LSV_AUC_RDT >  :LSV_AUC_GBDT 
	THEN
        INSERT INTO "MODELREPOSITORY" select 'CUSTOMER_CHURN','99.9', 'RDT', RDT."ROW_INDEX", RDT."TREE_INDEX", RDT."MODEL_CONTENT", NULL, NULL, 
              current_date, current_time 
         from PAL_RDT_MODEL_TBL as RDT;
    ELSE
        INSERT INTO "MODELREPOSITORY" select 'CUSTOMER_CHURN','99.9', 'GBDT', GBDT."ROW_INDEX", NULL, NULL, GBDT."KEY", GBDT."VALUE", 
              current_date, current_time 
         from PAL_GBDT_MODEL_TBL as GBDT;
    END IF;
    
end;
--Select * from  "MODELREPOSITORY" WHERE "MODEL_VERSION" > 99;
Select distinct "MODEL_NAME", "MODEL_VERSION", "MODEL_TYPE", "MODEL_DATE",  "MODEL_TIME" from  "MODELREPOSITORY" WHERE "MODEL_VERSION" > 99;

/***********************************************************************************************************************************************/










