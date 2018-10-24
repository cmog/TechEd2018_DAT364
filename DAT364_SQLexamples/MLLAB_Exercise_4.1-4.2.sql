/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                  ***/
/***/

/*** CODE-STEP 23 ***/
/*** Exercises 4 - Optimize prediction calls for performance      **************************************************************/
/*** Input from Step2  ***/
---- SAVED Model in MLLAB_###."MODELREPOSITORY";

/*******************************************************************************************************************************/
/*** Step 4.1 GET MODEL from MODELREPOSITORY  ***/
drop table #PRED_PAL_RDT_MODEL_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #PRED_PAL_RDT_MODEL_TBL ("ROW_INDEX" INTEGER,"TREE_INDEX" INTEGER,"MODEL_CONTENT" NVARCHAR(5000));
--CREATE LOCAL TEMPORARY COLUMN TABLE #PREDPAL_GBDT_MODEL_TBL ( "ROW_INDEX" INTEGER,	"KEY" NVARCHAR(1000),	"VALUE" NVARCHAR(1000));
insert into #PRED_PAL_RDT_MODEL_TBL 
   SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from "MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
 
--select * from  #PRED_PAL_RDT_MODEL_TBL;
select "ROW_INDEX",  "TREE_INDEX", CAST("MODEL_CONTENT" as VARCHAR(1000)) from "#PRED_PAL_RDT_MODEL_TBL";

/*** CODE-STEP 24 ***/
/*******************************************************************************************************************************/
/*** Step 4.2 PREDICT with MODEL on Partitioned input data for parallelization ***/
/*** Table "MLLAB"."NEWCUSTOMER_CHURN_ABT_PARTITIONED" is  'HASH 8 AccountID' - PARTITIONED **/

/*** STEP: Explore Table partitioning  ***/
SELECT HOST, SCHEMA_NAME, TABLE_NAME, PART_ID, RECORD_COUNT FROM M_CS_TABLES 
     WHERE  SCHEMA_NAME ='MLLAB' and TABLE_NAME='NEWCUSTOMER_CHURN_ABT_PARTITIONED';

/*** STEP: Prepare Parameters and result table ***/
DROP TABLE #PAL_PARAMETER_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL);

drop table #RESULT;
CREATE LOCAL TEMPORARY COLUMN TABLE #RESULT (
	"ID" INTEGER,
	"SCORE" NVARCHAR(100),
	"CONFIDENCE" Double
);
truncate table #RESULT; 

/*******************************************************************************************************************************/
/*** CODE-STEP 25 ***/
/*** STEP Parallel-enabled Prediction on partitioned tables   ***/
/*** USE: WITH HINT (PARALLEL_BY_PARAMETER_PARTITIONS(p1))    ***/

--- Use hint on regular input view;
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW, #PRED_PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview WITH HINT (PARALLEL_BY_PARAMETER_PARTITIONS(p1)) ;
/* --general warning: Call "_SYS_AFL"."PAL_RANDOM_DECISION_TREES_PREDICT": Parallelization by partitions possible only if a physical table is passed to parameter P1 */

--- Use hint wiht partitioned input data;
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT ("MLLAB"."NEWCUSTOMER_CHURN_ABT_PARTITIONED", #PRED_PAL_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, #RESULT) with overview WITH HINT (PARALLEL_BY_PARAMETER_PARTITIONS(p1)) ;
/* --general warning: Call "_SYS_AFL"."PAL_RANDOM_DECISION_TREES_PREDICT" will be parallelized by partitions of table "MLLAB"."NEWCUSTOMER_CHURN_ABT_PARTITIONED" passed to parameter P1, 
                      but parallelized result write is possible only if a column table is passed to the output parameter using the WITH OVERVIEW clause*/

					  

/*** For reference only, create of the partitioned table   ***/
/*CREATE COLUMN TABLE "MLLAB"."NEWCUSTOMER_CHURN_ABT_PARTITIONED" ("AccountID" INTEGER CS_INT,
	 "ServiceType" VARCHAR(21),
	 "ServiceName" VARCHAR(14),
	 ...
	 "Device_Lifetime_PPM" INTEGER CS_INT
       ) UNLOAD PRIORITY 5 AUTO MERGE 
	 PARTITION BY 'HASH 8 AccountID';*/
	 

/*******************************************************************************************************************************/
/*** CODE-STEP 26 ***/
/*** Step 4.3 PREDICT with MODEL parsed and loaded as in-memory runtime object for real-time predictions ***/

--#PRED_PAL_RDT_MODEL_TBL created in code-step 23;
/*SET SCHEMA MLLAB_00#;
CREATE LOCAL TEMPORARY COLUMN TABLE #PRED_PAL_RDT_MODEL_TBL ("ROW_INDEX" INTEGER,"TREE_INDEX" INTEGER,"MODEL_CONTENT" NVARCHAR(5000));
insert into #PRED_PAL_RDT_MODEL_TBL 
   SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from "MODELREPOSITORY" 
   WHERE "MODEL_NAME"='CUSTOMER_CHURN' AND "MODEL_VERSION"='1.1' AND "MODEL_TYPE"='RDT';
--select * from  #PRED_PAL_RDT_MODEL_TBL;
select "ROW_INDEX",  "TREE_INDEX", CAST("MODEL_CONTENT" as VARCHAR(1000)) from "#PRED_PAL_RDT_MODEL_TBL";
*/

/** create LOAD-State tables **/
--DROP TABLE #PAL_SET_STATE_PARAMETERS_TBL;
--CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_SET_STATE_PARAMETERS_TBL LIKE PAL_PARAMETER_T;

CREATE LOCAL TEMPORARY COLUMN TABLE  #PAL_SET_STATE_PARAMETERS_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);
INSERT INTO #PAL_SET_STATE_PARAMETERS_TBL VALUES('ALGORITHM', 2, NULL, NULL); --2: Random decision tree;
INSERT INTO #PAL_SET_STATE_PARAMETERS_TBL VALUES('STATE_DESCRIPTION', NULL, NULL, 'PAL RDT Churn-Model Version 1.1 AUTHOR: MLLAB_###');

--DROP TABLE PAL_EMPTY_TBL;
CREATE TABLE PAL_EMPTY_TBL( 
	ID double
);

--DROP TABLE PAL_STATE_TBL;
CREATE TABLE PAL_STATE_TBL (	
    S_KEY VARCHAR(50), 
	S_VALUE VARCHAR(100)
);



/** Create the model state for real-time prediction **/
CALL _SYS_AFL.PAL_CREATE_MODEL_STATE(#PRED_PAL_RDT_MODEL_TBL , PAL_EMPTY_TBL, PAL_EMPTY_TBL, PAL_EMPTY_TBL, PAL_EMPTY_TBL, #PAL_SET_STATE_PARAMETERS_TBL, PAL_STATE_TBL) WITH OVERVIEW;
select * from PAL_STATE_TBL;
select * from SYS.M_AFL_STATES WHERE DESCRIPTION ='PAL RDT Churn-Model Version 1.1 AUTHOR: MLLAB_###';

D01B88D8E9E75248898AC3DD637161D6
/* Reference to the parsed in-memory model during prediction */

/*** CODE-STEP 27 ***/
/*** Use parsed and loaded model as in-memory runtime object for real-time predictions ***/
CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_EMPTY_RDT_MODEL_TBL like #PRED_PAL_RDT_MODEL_TBL;

/*
CREATE LOCAL TEMPORARY COLUMN TABLE  #PAL_PARAMETER_TBL (
	"PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100)
);*/
Truncate TABLE #PAL_PARAMETER_TBL;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('THREAD_RATIO', NULL, 0.5, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VERBOSE', 0, NULL, NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('STATE_ID', NULL, NULL, '7168D7EB9B7CC5409E2A48D1BC16CCC1');


CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (NEWCUSTOMER_CHURN_ABT_VIEW, #PAL_EMPTY_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, ?);
	 
/** Predict with a single customer **/
create view MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW_1 as select * from NEWCUSTOMER_CHURN_ABT_VIEW where "AccountID"='2001101';
CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW_1, #PAL_EMPTY_RDT_MODEL_TBL, #PAL_PARAMETER_TBL, ?);


/***************************************************************************************************************************/
/*** Appendix - dynamically enable capture of STATE_ID as part of the PREDICT Call, not yet optimized for performance ******/	 
do 
begin  
  DECLARE "LSV_STATE_ID"  VARCHAR(100);

  DECLARE "PARAM_NAME" VARCHAR(100) ARRAY;
 	DECLARE "INT_VALUE" INTEGER ARRAY;
   	DECLARE "DOUBLE_VALUE" DOUBLE ARRAY;
   	DECLARE "STRING_VALUE" VARCHAR(100) ARRAY;
	
	SELECT "STATE_ID" into LSV_STATE_ID from SYS.M_AFL_STATES WHERE DESCRIPTION ='PAL RDT Churn-Model Version 1.1 AUTHOR: MLLAB_###';
	
    PARAM_NAME[1] := 'THREAD_RATIO';
	INT_VALUE[1] := NULL;
	DOUBLE_VALUE[1] := 0.5;   
	STRING_VALUE[1] := NULL; 
    PARAM_NAME[2] := 'VERBOSE';
    INT_VALUE[2] := 0;
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[2] := NULL;  
	PARAM_NAME[2] := 'STATE_ID';
    INT_VALUE[2] := NULL;
    DOUBLE_VALUE[2] := NULL;  
    STRING_VALUE[2] := :LSV_STATE_ID;  
    LT_PARMS = UNNEST(:PARAM_NAME, :INT_VALUE, :DOUBLE_VALUE, :STRING_VALUE) AS (PARAM_NAME, INT_VALUE, DOUBLE_VALUE, STRING_VALUE);  
    
	LT_SCOREDATA= SELECT * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW where "AccountID"='2001101';
    LT_EMPTY_MODELTAB=  SELECT "ROW_INDEX", "TREE_INDEX", "MODEL_CONTENT" from MLLAB_###."MODELREPOSITORY" WHERE "MODEL_NAME"='EMPTY_DUMMY';
 
    CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:LT_SCOREDATA, :LT_EMPTY_MODELTAB, :LT_PARMS , LT_SCORESET) ;
	
	SELECT "AccountID" as "ACCOUNTID", "SCORE" as "PREDICTION", "CONFIDENCE" as "PRED_CONFIDENCE" from :LT_SCORESET;
    
end;	
