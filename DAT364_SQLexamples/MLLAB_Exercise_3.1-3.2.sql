/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                  ***/
/***/
/*** Exercises 3 - Predict on New Customer, current Customer data *******************************************************************/
/*** Exercise objective: We want to train a classification model to predict customer churn using the Random Decision Tree     ***/
/***                     algorithm.                                                                                          ***/
/***                     For NEWCUSTOMER_CHURN_ABT_VIEW, the churn behavior is not known, data is from a new customer or     ***/
/***                     current month, describing the current customer state based on which the customer shall be           ***/
/***                     classified as likely to churn or not using our best predictive model created.                       ***/
/***                     Subtasks are: Get the model, create a prediction, embed a prediction call into a function.          ***/

/*** Input from Step1 and Step2                       ***/ 
--- NEWCUSTOMER Prediction View:  MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW
---SAVED Model in MLLAB_###."MODELREPOSITORY";

/*** Tasks for Exercises 3  ***/
-- predict
-- build pred in applications into TF / CalcView / CDS View with parameter ...

/*** CODE-STEP 17 ***/
/*******************************************************************************************************************************/
/*** Step 3.1 GET MODEL from MODELREPOSITORY  ***/

CREATE LOCAL TEMPORARY COLUMN TABLE #PRED_PAL_RDT_MODEL_TBL ("ROW_INDEX" INTEGER,"TREE_INDEX" INTEGER,"MODEL_CONTENT" NVARCHAR(5000));
insert into #PRED_PAL_RDT_MODEL_TBL 
   SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from "MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
--select * from  #PRED_PAL_RDT_MODEL_TBL;
select "ROW_INDEX",  "TREE_INDEX", CAST("MODEL_CONTENT" as VARCHAR(1000)) from "#PRED_PAL_RDT_MODEL_TBL";


-- case GBDT model;
/*   
CREATE LOCAL TEMPORARY COLUMN TABLE #PREDPAL_GBDT_MODEL_TBL ( "ROW_INDEX" INTEGER,	"KEY" NVARCHAR(1000),	"VALUE" NVARCHAR(1000));#
--truncate table #PREDPAL_GBDT_MODEL_TBL;
insert into #PREDPAL_GBDT_MODEL_TBL 
   SELECT "ROW_INDEX", "KEY", "VALUE" from "MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='GBDT';
select * from  #PREDPAL_GBDT_MODEL_TBL;*/


/*** CODE-STEP 18 ***/
/*******************************************************************************************************************************/
/*** Step 3.2 PREDICT with MODEL ***/
--Preditc with Random Decision Trees;
/*DROP TABLE #PAL_PARAMETER_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);*/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL);

-- Note, confidence is DOUBLE with RDT algorithm, NVARCHAR for the GBDT algorithm;
--drop table #RESULT;
/*CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" Double
);*/
truncate table #RESULT;
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW, #PRED_PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview;

Select count (distinct "SCORE") from #RESULT;
select * from #result order by "SCORE", "CONFIDENCE" DESC;
Select count (*) as NCount, "SCORE" from #RESULT group by "SCORE"; --14 churned;
Select count (*) as NCount from #RESULT where "SCORE"='Churned';
select count (*) from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;
select * from #RESULT where "ID"=2001727;
Select * from #RESULT where "SCORE"='Churned' order by "ID";
Select * from #RESULT where "SCORE"='Churned' order by "CONFIDENCE" DESC;
Select * from #RESULT  order by "ID";

select * from #RESULT where "ID" in (2001727, 2001675, 2001539, 2001680, 2001540, 2001538, 2001644) order by "ID";

--IDs with highest Churn confidence (>0.5)
--2001727, 2001675, 2001539, 2001680, 2001540, 2001538, 2001644

/*** CODE-STEP 19 ***/
/*******************************************************************************************************************************/
/*** Step 3.3 Predict within Table Function           ***/

/*** STEP: develop and test your SQLScript function code using anonymous blocks  ***/
do 
begin  
    DECLARE "PARAM_NAME" VARCHAR(100) ARRAY;
   	DECLARE "INT_VALUE" INTEGER ARRAY;
   	DECLARE "DOUBLE_VALUE" DOUBLE ARRAY;
   	DECLARE "STRING_VALUE" VARCHAR(100) ARRAY;
	
    PARAM_NAME[1] := 'THREAD_RATIO';
    PARAM_NAME[2] := 'VERBOSE';
    INT_VALUE[1] := NULL;
    INT_VALUE[2] := 0;
    DOUBLE_VALUE[1] := 0.5;   
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[1] := NULL;  
    STRING_VALUE[2] := NULL;  
  LT_PARMS = UNNEST(:PARAM_NAME, :INT_VALUE, :DOUBLE_VALUE, :STRING_VALUE) AS (PARAM_NAME, INT_VALUE, DOUBLE_VALUE, STRING_VALUE);
  
 LT_SCOREDATA= SELECT * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;
 LT_MODELTAB=  SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from MLLAB_###."MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
 
 CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:LT_SCOREDATA, :LT_MODELTAB, :LT_PARMS , LT_SCORESET) ;

  SELECT "AccountID" as "ACCOUNTID", "SCORE" as "PREDICTION", "CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET;

end;

/*** CODE-STEP 20 ***/
/*** STEP: Function to predict over all customers  ***/
--drop function "MLLAB_###"."TUDF_PREDICT_CUSTOMER_CHURN";
CREATE FUNCTION "MLLAB_###"."TUDF_PREDICT_CUSTOMER_CHURN" ( ) 
		RETURNS TABLE (ACCOUNTID INTEGER, PREDICTION NVARCHAR(100), PRED_CONFIDENCE Double)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
/***************************** 
	Write your function logic
 *****************************/
   	DECLARE "PARAM_NAME" VARCHAR(100) ARRAY;
   	DECLARE "INT_VALUE" INTEGER ARRAY;
   	DECLARE "DOUBLE_VALUE" DOUBLE ARRAY;
   	DECLARE "STRING_VALUE" VARCHAR(100) ARRAY;
	
    PARAM_NAME[1] := 'THREAD_RATIO';
    PARAM_NAME[2] := 'VERBOSE';
    INT_VALUE[1] := NULL;
    INT_VALUE[2] := 0;
    DOUBLE_VALUE[1] := 0.5;   
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[1] := NULL;  
    STRING_VALUE[2] := NULL;  
  LT_PARMS = UNNEST(:PARAM_NAME, :INT_VALUE, :DOUBLE_VALUE, :STRING_VALUE) AS (PARAM_NAME, INT_VALUE, DOUBLE_VALUE, STRING_VALUE);
  
 LT_SCOREDATA= SELECT * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;
 LT_MODELTAB=  SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from MLLAB_###."MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
 
 CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:LT_SCOREDATA, :LT_MODELTAB, :LT_PARMS , LT_SCORESET);

 RETURN SELECT "AccountID" as "ACCOUNTID", "SCORE" as "PREDICTION", "CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET;

 
END;

/*** STEP: Call the Function to predict over all customers  ***/
select * from "MLLAB_###"."TUDF_PREDICT_CUSTOMER_CHURN"();


/*** CODE-STEP 21 ***/
/*** STEP: Function to predict for a single customers using Customer_ID Input Parameter  ***/
--drop function "MLLAB_###"."TUDF_PREDICT_SINGLECUSTOMER_CHURN";
CREATE FUNCTION "MLLAB_###"."TUDF_PREDICT_SINGLECUSTOMER_CHURN" ( IN IP_ID INTEGER ) 
		RETURNS TABLE (ACCOUNTID INTEGER, PREDICTION NVARCHAR(100), PRED_CONFIDENCE Double)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
/***************************** 
	Write your function logic
 *****************************/
   	DECLARE "PARAM_NAME" VARCHAR(100) ARRAY;
   	DECLARE "INT_VALUE" INTEGER ARRAY;
   	DECLARE "DOUBLE_VALUE" DOUBLE ARRAY;
   	DECLARE "STRING_VALUE" VARCHAR(100) ARRAY;
	
    PARAM_NAME[1] := 'THREAD_RATIO';
    PARAM_NAME[2] := 'VERBOSE';
    INT_VALUE[1] := NULL;
    INT_VALUE[2] := 0;
    DOUBLE_VALUE[1] := 0.5;   
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[1] := NULL;  
    STRING_VALUE[2] := NULL;  
  LT_PARMS = UNNEST(:PARAM_NAME, :INT_VALUE, :DOUBLE_VALUE, :STRING_VALUE) AS (PARAM_NAME, INT_VALUE, DOUBLE_VALUE, STRING_VALUE);
  
 LT_SCOREDATA= SELECT * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW WHERE "AccountID"= :IP_ID;
 LT_MODELTAB=  SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from MLLAB_###."MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
 
 CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:LT_SCOREDATA, :LT_MODELTAB, :LT_PARMS , LT_SCORESET);

 RETURN SELECT "AccountID" as "ACCOUNTID", "SCORE" as "PREDICTION", "CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET; 
END;

--- Explore the function parameters;
select * from "PUBLIC"."FUNCTION_PARAMETERS" 
where function_name ='TUDF_PREDICT_SINGLECUSTOMER_CHURN';

select * from "PUBLIC"."FUNCTION_PARAMETER_COLUMNS" 
where function_name ='TUDF_PREDICT_SINGLECUSTOMER_CHURN';


/*** STEP: Function to predict for a single customers using Customer_ID Input Parameter  ***/
select * from "MLLAB_###"."TUDF_PREDICT_SINGLECUSTOMER_CHURN"(2001644);
--IDs with highest Churn confidence (>0.5)
--2001727, 2001675, 2001539, 2001680, 2001540, 2001538, 2001644

/*** CODE-STEP 22 ***/
/*** Note, in order to leverage the Type-Any PAL procedures in depending development artefacts like Calculation Views or JavaScript calls,							***/
/*** the PAL procedure call is required to be embedded within a SQLScriptV2 object. Since SAP HANA 2 SPS02, SQLScript Procedures of type SQLScriptV2 are generated. ***/

-- Step a) Create the Procedure
CREATE PROCEDURE "MLLAB_###"."PROC_PREDICT_CUSTOMER_CHURN" (
	 out t TABLE (ACCOUNTID INTEGER, PREDICTION NVARCHAR(100), PRED_CONFIDENCE Double) )
        LANGUAGE SQLSCRIPT
        
        SQL SECURITY INVOKER 
        READS SQL DATA AS
	BEGIN
	/***************************** 
        Write your function logic
	*****************************/
        DECLARE "PARAM_NAME" VARCHAR(100) ARRAY;
        DECLARE "INT_VALUE" INTEGER ARRAY;
        DECLARE "DOUBLE_VALUE" DOUBLE ARRAY;
        DECLARE "STRING_VALUE" VARCHAR(100) ARRAY;
        
    PARAM_NAME[1] := 'THREAD_RATIO';
    PARAM_NAME[2] := 'VERBOSE';
    INT_VALUE[1] := NULL;
    INT_VALUE[2] := 0;
    DOUBLE_VALUE[1] := 0.5;   
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[1] := NULL;  
    STRING_VALUE[2] := NULL;  
	LT_PARMS = UNNEST(:PARAM_NAME, :INT_VALUE, :DOUBLE_VALUE, :STRING_VALUE) AS (PARAM_NAME, INT_VALUE, DOUBLE_VALUE, STRING_VALUE);
  
	LT_SCOREDATA= SELECT * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;
	LT_MODELTAB=  SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from MLLAB_###."MODELREPOSITORY" 
	WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';

	CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:LT_SCOREDATA, :LT_MODELTAB, :LT_PARMS , LT_SCORESET);

	t = SELECT "AccountID" as "ACCOUNTID", "SCORE" as "PREDICTION", "CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET;
END;

Call          "MLLAB_###"."PROC_PREDICT_CUSTOMER_CHURN"(?) ;


-- Step b) Wrap the procedure within a table function
CREATE FUNCTION "MLLAB_###"."TUDF_PREDICT_WRAPPER" ( ) 
		RETURNS TABLE (ACCOUNTID INTEGER, PREDICTION NVARCHAR(100), PRED_CONFIDENCE Double)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER AS
BEGIN
	/***************************** 
		Write your function logic
	*****************************/
	call "MLLAB_###"."PROC_PREDICT_CUSTOMER_CHURN"(LT_SCORESET) ;
 
	RETURN SELECT "ACCOUNTID" as "ACCOUNTID", "PREDICTION" as "PREDICTION", "PRED_CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET;

END;


select * from "MLLAB_###"."TUDF_PREDICT_WRAPPER"();


/*** Results: ***/
-- Loaded model from MODELREPOSITORY and PREDICTION CALL with NEWCUSTOMER_CHURN_ABT_VIEW
-- Function "TUDF_PREDICT_CUSTOMER_CHURN"
-- Function "TUDF_PREDICT_SINGLECUSTOMER_CHURN"



