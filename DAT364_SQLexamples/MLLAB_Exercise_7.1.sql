/*******************************************************************************************************************************/
/*** SAP TechEd 2018 - HandsOn session Developing Smart Applications using SAP HANA In-Database Machine Learning            ***/
/*** Prepared by Christoph Morgen, SAP SE - 4st August 2018                                                                 ***/


/*** OPTIONAL EXERCISE 7.1       **********************************************************************************************/
/*** Using the TensorFlow integration in SAP HANA to embed a image classification model within a SAP HANA application ********/

/*** CODE-STEP 70 ***/
/*** Inspect the EML-Functions available ***/
SELECT * FROM "SYS"."AFL_FUNCTIONS" WHERE AREA_NAME = 'EML' AND "FUNCTION_NAME" not like '%OVER%' ;


/*** Overview the image data as stored in the SAP HANA table ***/
/*** Images are stored HEX encoded in the HANA table         ***/
select * from MLLAB_TF.IMAGES_RAW_HEX ORDER by NR;


/*** CODE-STEP 71 ***/
/*** Review and validate configuration setup and Tensorflow-Modelserver serviced classification models ***************/
/*** An admin user has registered the Tensorflow-Modelserver as a remote source and                    ***************/
/***               registered the available models, within the EML_MODEL_CONFIGURATION table           ***************/
select * from  _SYS_AFL.EML_MODEL_CONFIGURATION;


/*** Validating if a given model (sourced name/value) is available at the Tensorflow-Modelserver remote source *******/
Select * from MLLAB_TF.PREDICTPARMS_SLEEVESMODEL;

--DROP TABLE #EML_CHECKDESTINATION_RESULTS;
CREATE LOCAL TEMPORARY TABLE #EML_CHECKDESTINATION_RESULTS ("Code" VARCHAR(10), "Longtext" VARCHAR(100));
truncate table #EML_CHECKDESTINATION_RESULTS;
Call _SYS_AFL.EML_CHECKDESTINATION_PROC(MLLAB_TF.PREDICTPARMS_SLEEVESMODEL,#EML_CHECKDESTINATION_RESULTS) with overview;
Call _SYS_AFL.EML_CHECKDESTINATION_PROC(MLLAB_TF.PREDICTPARMS_COLORMODEL,#EML_CHECKDESTINATION_RESULTS) with overview;
select * from #EML_CHECKDESTINATION_RESULTS;


/*** CODE-STEP 72 ***/
/*** Call the TensorFlow sleeve-type and sleeve-color classification models with different shirt images **************/

/*** This TensorFlow models, expect the images for classification  to be in jpg (binary/string),        **************/
/*** hence while selecting the image, we are converting it to binary before passing it into the call.   **************/
create LOCAL TEMPORARY table #IMAGE2CLASSIFY like MLLAB_TF.IMAGES_BIN_TT;
truncate table #IMAGE2CLASSIFY;
insert into #IMAGE2CLASSIFY ("image") select HEXTOBIN("IMAGE") 
	from MLLAB_TF."IMAGES_RAW_HEX" where "NR" = '01';
select * from #IMAGE2CLASSIFY;

/*** Passing the image and model information to the call for classification                             **************/
Create local temporary table #RESULT_PREDICTSLEEVETYPE like MLLAB_TF.RESULT_PREDICTSLEEVETYPE;
truncate table #RESULT_PREDICTSLEEVETYPE;
CALL MLLAB_TF.TFEML_PREDICT_TSHIRTSLEEVES(MLLAB_TF.PREDICTPARMS_SLEEVESMODEL, #IMAGE2CLASSIFY, #RESULT_PREDICTSLEEVETYPE) with overview;
select * from #RESULT_PREDICTSLEEVETYPE;

Create local temporary table #RESULT_PREDICTCOLOR like MLLAB_TF.RESULT_PREDICTCOLOR;
truncate  table #RESULT_PREDICTCOLOR;
CALL MLLAB_TF.TFEML_PREDICT_TSHIRTCOLOR(MLLAB_TF.PREDICTPARMS_COLORMODEL, #IMAGE2CLASSIFY, #RESULT_PREDICTCOLOR) with overview;
select * from #RESULT_PREDICTCOLOR;

/*** Repeat the classification with shirt number of your choice and inspect the classification results  */