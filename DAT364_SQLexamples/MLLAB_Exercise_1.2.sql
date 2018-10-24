*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 1st August 2018                                                                  ***/
/*** Step 1 - Data Preparation *************************************************************************************************/
/*** Scenario objective: Develop a customer churn prediction model using different PAL-classification algorithms              ***/
/***                     Embed the classification prediction functions into table functions for use within applications.      ***/
/***                     Given is a data set (analytical base table) CUSTOMER_CHURN_ABT, with insight about each customer    ***/
/***                     and measure on current, previous month (_PM) and previous-previous month (_PPM) observations.        ***/
/***                     For CUSTOMER_CHURN_ABT the churn behavior is known (ContractActivity), thus it is used for training ***/
/***                     the classification predictive model.                                                                 ***/
/***                     For NEWCUSTOMER_CHURN_ABT_VIEW, the churn behavior is not known, data is from a new or current      ***/
/***                     month, describing the current customer state.                                                       ***/
/*** Exercise objective: Creating the input data views for the predictive modeling process                                   ***/
/***                     Select the relevant columns, add further derived column for better predictive modeling results, ... ***/
/***                     Furthermore, the input data is split into 3 random sampled partition:                               ***/
/***                     The train-partition is used to train the algorithm for the pattern, the validate-partition is       ***/
/***                     used to validate against the trained model, tune and re-iterate the algorithm training, in order    ***/
/***                     to not overfit the model towards the train-partition, is must show good prediction results also     ***/
/***                     against data not seen in the training, i.e. the validate-partition.                                 ***/
/***                     The test-partition is then used to cross-compare different trained algorithm using a neutral        ***/
/***                     set of data, which yet hasn't been involved in training or model validation.                        ***/
/*******************************************************************************************************************************/

/*******************************************************************************************************************************/
/*** Step 1a - Create Input views for training the classification algorithm and for the later prediction ***/
/*** Note, it would be common to derive additional calculated columns                                    ***/
/*** CODE-STEP 01 ***/
--SET SCHEMA MLLAB_###;
select * from "MLLAB"."CUSTOMER_CHURN_ABT";

--drop view  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW;
CREATE VIEW MLLAB_###.CUSTOMER_CHURN_ABT_VIEW
as select "AccountID",	"ServiceType" ,	 "ServiceName",	 "DataAllowance_MB",	 "VoiceAllowance_Minutes" ,	 "SMSAllowance_N_Messages" ,
	 "DataUsage_PCT" ,	 "DataUsage_PCT_PM",	  "DataUsage_PCT_PPM" ,
	 "VoiceUsage_PCT" , 	 "VoiceUsage_PCT_PM" , 	 "VoiceUsage_PCT_PPM" ,
	 "SMSUsage_PCT" , 	 "SMSUsage_PCT_PM" , 	 "SMSUsage_PCT_PPM" ,
	 "Revenue_Month" ,	  "Revenue_Month_PM",	   "Revenue_Month_PPM" ,	    "Revenue_Month_PPPM" ,
	 "ServiceFailureRate_PCT" ,	 "ServiceFailureRate_PCT_PM" ,	 "ServiceFailureRate_PCT_PPM" ,
	 "CustomerLifetimeValue_USD" ,	  "CustomerLifetimeValue_USD_PM" ,	   "CustomerLifetimeValue_USD_PPM" ,
	 "Device_Lifetime" ,	 	 "Device_Lifetime_PM" ,	 	 	 "Device_Lifetime_PPM" ,
	 "ContractActivityLABEL" 
	 from "MLLAB"."CUSTOMER_CHURN_ABT";
select * from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW;

--drop view  MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;
CREATE VIEW MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW
as select "AccountID",	"ServiceType" ,	 "ServiceName",	 "DataAllowance_MB",	 "VoiceAllowance_Minutes" ,	 "SMSAllowance_N_Messages" ,
	 "DataUsage_PCT" ,	 "DataUsage_PCT_PM",	  "DataUsage_PCT_PPM" ,
	 "VoiceUsage_PCT" , 	 "VoiceUsage_PCT_PM" , 	 "VoiceUsage_PCT_PPM" ,
	 "SMSUsage_PCT" , 	 "SMSUsage_PCT_PM" , 	 "SMSUsage_PCT_PPM" ,
	 "Revenue_Month" ,	  "Revenue_Month_PM",	   "Revenue_Month_PPM" ,	    "Revenue_Month_PPPM" ,
	 "ServiceFailureRate_PCT" ,	 "ServiceFailureRate_PCT_PM" ,	 "ServiceFailureRate_PCT_PPM" ,
	 "CustomerLifetimeValue_USD" ,	  "CustomerLifetimeValue_USD_PM" ,	   "CustomerLifetimeValue_USD_PPM" ,
	 "Device_Lifetime" ,	 	 "Device_Lifetime_PM" ,	 	 	 "Device_Lifetime_PPM" 
	 from "MLLAB"."NEWCUSTOMER_CHURN_ABT";
select * from MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;

/*******************************************************************************************************************************/
/*** Step 1b - Create TRAIN, VALIDATE, TEST Partition and Views ***/
/*** The training input data shall be split into 3 partitions, using a stratified sampling approach, to ensure equal         ***/
/*** proportions of positive labels                                                                                          ***/
/*** Calling the new PAL_PARTITION procedure from _SYS_AFL-Schema to create the Train, Validate and Test-subsets             ***/
/*** CODE-STEP 02 ***/
----DROP TABLE #PAL_PARAMETER_TBL;
CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_PARAMETER_TBL (
    "PARAM_NAME" VARCHAR (256),
    "INT_VALUE" INTEGER,
    "DOUBLE_VALUE" DOUBLE,
    "STRING_VALUE" VARCHAR (1000)
);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('PARTITION_METHOD',1,null,null); --stratified sample;
INSERT INTO #PAL_PARAMETER_TBL VALUES ('RANDOM_SEED',23,null,null);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('HAS_ID',1,NULL,NULL);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('STRATIFIED_COLUMN',null,null,'ContractActivityLABEL');
INSERT INTO #PAL_PARAMETER_TBL VALUES ('TRAINING_PERCENT', null,0.6,null);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('TESTING_PERCENT', null,0.2,null);
INSERT INTO #PAL_PARAMETER_TBL VALUES ('VALIDATION_PERCENT', null,0.2,null);

--- The actual PAL-procedure call, no wrapper procedure use required anymore.;
--- Firstly, explore the PAL-procedure use and results, without specifying any defined output;
CALL "_SYS_AFL"."PAL_PARTITION"(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW, #PAL_PARAMETER_TBL, ?) ;
--DROP TABLE MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS;

/*** CODE-STEP 03 ***/
--- Now we want to, save the partition output into a table, therefore prepare a table to do so;
CREATE  COLUMN TABLE MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS (
    "AccountID" INTEGER,
    "PARTITION_TYPE" INTEGER
);
CALL "_SYS_AFL"."PAL_PARTITION"(MLLAB_###.CUSTOMER_CHURN_ABT_VIEW, #PAL_PARAMETER_TBL, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS) with overview ;
select count(*), "PARTITION_TYPE" from MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS group by "PARTITION_TYPE";
select count(*), P."PARTITION_TYPE", D."ContractActivityLABEL" 
     from MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P, MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as D
     where P."AccountID"=D."AccountID"
     group by "PARTITION_TYPE", D."ContractActivityLABEL" ;

	 
/*** CODE-STEP 04 ***/
--- Finally, we join the partition information based on the AccountID back with the original training input view, to get 3 subset views with the train-, validate- and test-subsets;    
Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE
as select C.* from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=1;
select count(*), "ContractActivityLABEL" from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE group by "ContractActivityLABEL";

Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE
as select C.* from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=3;
select count(*), "ContractActivityLABEL" from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE group by "ContractActivityLABEL";

Create View MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE
as select C.* from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW as C, MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS as P
where C."AccountID"=P."AccountID" and P."PARTITION_TYPE"=2;
select count(*), "ContractActivityLABEL" from MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE group by "ContractActivityLABEL";

/*** Results: ***/
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW;
--- MLLAB_###.NEWCUSTOMER_CHURN_ABT_VIEW;; 
--- MLLAB_###.CUSTOMER_CHURN_ABT_PARTITIONS;
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TRAINSAMPLE;
---  MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_VALSAMPLE;
--- MLLAB_###.CUSTOMER_CHURN_ABT_VIEW_TESTSAMPLE;



