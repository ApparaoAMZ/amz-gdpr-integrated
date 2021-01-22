package com.amazon.gdpr.util;

/****************************************************************************************
 * This Class contains all the constants that is required through out the project 
 ****************************************************************************************/
public class GlobalConstants {

	//Status
	public static String STATUS_INPROGRESS 		= "INPROGRESS";
	public static String STATUS_SUCCESS 		= "SUCCESS";
	public static String STATUS_FAILURE 		= "FAILURE";
	public static String STATUS_ACTIVE			= "ACTIVE";
	public static String STATUS_SCHEDULED		= "SCHEDULED";
	public static String STATUS_CLEARED			= "CLEARED";
	
	// Modules 
	public static String MODULE_PAGELOAD					= "Page Load Module";
	public static String MODULE_INITIALIZATION 				= "Initialization Module";
	public static String SUB_MODULE_RUN_INITIALIZE			= "Run Initialize Sub Module";
	public static String SUB_MODULE_ANONYMIZE_JOB_INITIALIZE	= "Anonymize Initialize Job Sub Module";
	public static String SUB_MODULE_BACKUP_TABLE_INITIALIZE	= "Backup Table Initialize Sub Module";
	public static String SUB_MODULE_REORGANIZE_JOB_INITIALIZE = "ReOrganize Input Data Job Sub Module"; 
	public static String SUB_MODULE_SUMMARY_DATA_INITIALIZE	= "Summary Data Initialize Sub Module";
	public static String SUB_MODULE_REORGANIZE_DATA			= "ReOrganize Input data Batch Sub Module";
	
	//public static String MODULE_REORGANIZEINPUT		= "ReOrganize Input Module";
	public static String MODULE_DATABACKUP 			= "DataBackup Module";
	//public static String MODULE_TAGGED				= "Tagging Module";	
	public static String MODULE_DEPERSONALIZATION 	= "Depersonalization Module";
	public static String SUB_MODULE_TAGGED_DATA			= "Archival Tables Tagging Data Sub Module";
	public static String SUB_MODULE_TAG_JOB_INITIALIZE = "Archival Tables Tagging Job Sub Module";
	public static String SUB_MODULE_ANONYMIZE_DATA		= "Anonymize Archival data Sub Module";
	
	public static String MODULE_DATAREFRESH 		= "DataRefresh Module";
	
	public static int INITIALIZATION_MODULE_STATUS_COUNT = 5;
	public static int BACKUP_MODULE_STATUS_COUNT = 6;
	public static int DEPERSONALIZATION_MODULE_STATUS_COUNT = 7;
		
	//Exception Categories
	public static String EXCEPTION_GENERAL 			= "General Exception";
	public static String EXCEPTION_SQLEXCEPTION 	= "SQL Exception";
	public static String EXCEPTION_URIEXCEPTION 	= "URI Exception";
	
	public static String NA_STRING	= "NA";
	public static String EMPTY_STRING 	= "";
	public static String SPACE_STRING   = " ";
	public static String SEMICOLON_STRING = "; ";
	public static String COMMA_STRING = ", ";
	public static String COMMA_ONLY_STRING = ",";
	
	//Controller Files
	public static String CLS_WELCOMEPAGECONTROLLER		= "WelcomePageController";
	public static String CLS_INPUTDETAILCONTROLLER		= "InputDetailController";
	public static String CLS_SALESFORCEDETAILCONTROLLER = "SalesforceDetailController";	
	public static String CLS_GDPRCONTROLLER 			= "GdprController";
	public static String CLS_GDPRPROCESSCONTROLLER		= "GdprProcessController";
	public static String CLS_GDPRDEPERSONALIZATIONCONTROLLER = "GdprDepersonalizationController";
	public static String CLS_FILEUPLOADCONTROLLER		= "FileUploadController";
	
	public static String CLS_GDPRAPPLICATION		= "GDPRApplication";
	public static String CLS_GDPRCMDLINEAPPLICATION = "GdprCmdLineApplication";
	
	public static String CLS_RUNMGMTPROCESSOR 		= "RunMgmtProcessor";
	public static String CLS_ANONYMIZATIONPROCESSOR = "AnonymizationProcessor";
	public static String CLS_GDPRDATAPROCESSOR		= "GdprDataProcessor";
	public static String CLS_SUMMARYDATAPROCESSOR   = "SummaryDataProcessor";
	public static String CLS_TAGQUERYPROCESSOR		= "TagQueryProcessor";			
	public static String CLS_MODULEMGMT_PROCESSOR 	= "ModuleMgmtProcessor";
	public static String CLS_DATALOAD_PROCESSOR		= "DataLoadProcessor";
	
	public static String CLS_JOB_REORGANIZEDATAPROCESSOR = "ReorganizeDataProcessor";
	public static String CLS_JOB_TAGGINGPROCESSOR = "TaggingProcessor";
	public static String CLS_JOB_ANONYMIZEPROCESSOR = "AnonymizeProcessor";
	
	public static String CLS_REORGANIZE_INPUT_PROCESSOR = "ReOrganizeInputProcessor";
	public static String CLS_TAGDATAPROCESSOR	= "TagDataProcessor";
	public static String CLS_DEPERSONALIZATIONPROCESSOR = "DepersonalizationProcessor";
	public static String CLS_TAGGEDJOBTHREAD	= "TaggedJobThread";
	public static String CLS_BACKUPSERVICE		= "BackupService";
	
	public static String CLS_DATABASECONFIG			= "DatabaseConfig";
	public static String CLS_GDPR_DEPERSONALIZATION_BATCH_CONFIG		= "GdprDepersonalizationBatchConfig";
	public static String CLS_REORGANIZEINPUT_BATCHCONFIG  = "ReOrganizeInputBatchConfig";
	public static String CLS_TAGGING_BATCH_CONFIG	= "TaggingBatchConfig";
	public static String CLS_ANONYMIZE_BATCH_CONFIG = "AnonymizeBatchConfig";
	
	public static String CLS_RUNMGMTDAOIMPL			= "RunMgmtDaoImpl";
	public static String CLS_GDPRINPUTDAOIMPL		= "GdprInputDaoImpl";
	public static String CLS_GDPRINPUTFETCHDAOIMPL	= "GdprInputFetchDaoImpl";
	public static String CLS_GDPROUTPUTDAOIMPL		= "GdprOutputDaoImpl";
	public static String CLS_HVHOUTPUTDAOIMPL		= "HvhOutputDaoImpl";
	public static String CLS_TAGINPUTDAOIMPL		= "TagInputDaoImpl";
	public static String CLS_REORGANIZEINPUTWRITER	= "ReorganizeOutputWriter";
	public static String CLS_TAGINPUTWRITER			= "TagInputWriter";
	public static String CLS_ANONYMIZEINPUTWRITER   = "AnonymizeInputWriter";
	
	public static String CLS_RUNMGMTROWMAPPER				= "RunMgmtRowMapper";
	public static String CLS_CATEGORYROWMAPPER				= "CategoryRowMapper";
	public static String CLS_IMPACTTABLEROWMAPPER			= "ImpactTableRowMapper";
	public static String CLS_IMPACTFIELDROWMAPPER			= "ImpactFieldRowMapper";
	public static String CLS_ANONYMIZATIONDETAILROwMAPPER 	= "AnonymizationDetailRowMapper";
	public static String CLS_SUMMARYDATAROWMAPPER			= "SummaryDataRowMapper";
	public static String CLS_COUNTRYROWMAPPER				= "CountryRowMapper";
	public static String CLS_RUNMODULEMGMTROWMAPPER			= "RunModuleMgmtRowMapper";
	public static String CLS_DATALOADROWMAPPER				= "DataLoadRowMapper";
	public static String CLS_RUNSUMMARYMGMT_ROWMAPPER 		= "RunSummaryMgmtRowMapper";
	
	/*****Backup Table Processor Code Change Starts***/
	public static String CLS_BACKUPTABLEPROCESSORDAOIMPL			= "BackupTableProcessorDaoImpl";
	public static String CLS_BACKUPTABLEPROCESSOR					= "BackupTableProcessor";
	public static String CLS_IMPACTTABLEDETAILSROWMAPPER			= "ImpactTableDetailsRowMapper";
	public static String CLS_BACKUPTABLEDETAILSROWMAPPER			= "CLS_BACKUPTABLEDETAILSROWMAPPER";
	public static String CLS_GDPRDEPERSONALIZATIONINPUTROWMAPPER	= "GdprDepersonalizationInputRowMapper";
	public static String CLS_ARCHIVETABLEROWMAPPER					= "ArchiveTableRowMapper";	
	public static String CLS_ANONYMIZETABLEROWMAPPER				= "AnonymizeTableRowMapper";
	
	
	public static String CLS_JOBCOMPLETIONLISTENER = "JobCompletionListener";
	public static String CLS_REORGANIZECOMPLETIONLISTENER = "ReorganizeInputCompletionListener";
	public static String CLS_TAGGINGCOMPLETIONLISTENER = "TaggingJobCompletionListener";
	public static String CLS_ANONYMIZECOMPLETIONLISTENER = "AnonymizeJobCompletionListener";
	
	/*****Backup Table Processor Code Change Ends***/
	public static String TEMPLATE_FILEUPLOAD				= "fileUpload";
	public static String TEMPLATE_GDPRINPUT					= "gdprInput";
	
	public static String ATTRIBUTE_MESSAGE					= "message";
	public static String ATTRIBUTE_GDPRINPUT				= "gdprInput";
	
	public static String IMPACTTABLE_MAP_IDTONAME			= "Id-Name";
	public static String IMPACTTABLE_MAP_NAMETOID			= "Name-Id";
	
	public static String COL_RUN_ID					= "RUN_ID";
	
	public static String TYPE_TABLE_PARENT		= "PARENT";
	public static String TYPE_TABLE_CHILD		= "CHILD";
	
	public static String KEY_CATEGORY_ID 		= "CATEGORY_ID";
	public static String KEY_COUNTRY_CODE		= "COUNTRY_CODE";
		
	public static String ANONYMIZATION_FILE_TYPE 	= "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
	public static String FILE_UPLOAD_LOCATION		= "//temp/GDPR/";
	
	public static String ERROR_FILE_MESSAGE 	= "Please upload a valid anonymization excel file. ";
	public static String ERROR_FILE_EMPTY 		= ERROR_FILE_MESSAGE + "Not file attached. ";
	public static String ERROR_FILE_NOTEXCEL	= ERROR_FILE_MESSAGE + "File attached is not excel. ";
	public static String ERROR_FILE_UPLOAD		= "File processing had an issue. Please verify logs. ";
	public static String SUCCESS_FILE_MESSAGE	= "File has been uploaded !!!";
	
	public static String DATE_FORMAT 			= "yyyy-MM-dd HH24:MI:ss";
	
	//File Upload Messages
	public static String MSG_FILE_UPLOAD		= "Please upload Anonymization File only. ";
	public static String FILE_UPLOAD_SUCCESS	= "File uploaded successfully! ";
	public static String FILE_SHEET_NAME		= "Sheet1";
	
	public static String DATE_DATATYPE 	= "Date";
	public static String TEXT_DATATYPE	= "TEXT";
	
	public static String CONVERSION_TYPE_PRIVACY_DELETED = "PRIVACY DELETED";
	
	public static String JOB_REORGANIZE_INPUT_PROCESSOR = "processreorganizeInputJob";
	public static String JOB_TAGGING  = "processTaggingJob";
	public static String JOB_ANONYMIZE = "processAnonymizeJob";
	public static String JOB_REORGANIZE_INPUT_CATEGORYMAP = "CategoryMap";	
	public static String JOB_REORGANIZE_INPUT_RUNID		 = "RunId";
	public static String JOB_REORGANIZE_INPUT_JOBID		 = "JobId";	
	public static String JOB_REORGANIZE_INPUT_COUNTRIES	 = "selectedCountries";
	public static String JOB_REORGANIZE_LOAD_DATE		 = "GDPR_TBL_LOAD_DATE";
	public static String JOB_REORGANIZE_INPUT_FIELDCATEGORYMAP = "FieldCategoryMap";
	public static String TBL_GDPR_DEPERSONALIZATION__C = "GDPR_DEPERSONALIZATION__C";
	public static String JOB_INPUT_RUN_ID   = "RunId";	
	public static String JOB_INPUT_JOB_ID   = "JobId";
	public static String JOB_INPUT_RUN_SUMMARY_ID = "RunSummaryId";
	public static String JOB_INPUT_TABLE_NAME = "TableName";
	public static String JOB_INPUT_COUNTRY_CODE = "CountryCode";
	public static String JOB_INPUT_CATEGORY_ID = "CategoryId";
	public static String JOB_INPUT_TAG_QRY = "TagQuery";
	
	//Error Messages
	public static int DUMMY_RUN_ID = 0;
	public static int ERROR_INITIAL_INDEX = 0;
	public static int ERROR_DETAIL_SIZE = 4999;
	public static int WAIT_TIME = 1000;
	
	public static String MSG_LOAD_FORM				= "GdprInput Form data loaded. ";
	public static String ERR_LOAD_FORM				= "Facing issues in loading GDPRInput Form. ";
	public static String ERR_DISPLAY 				= "Please verify log for further details.";
	public static String ERR_RUN_INITIALIZATION 	= "Run Initiation Exception. ";
	public static String ERR_OLD_RUN_FETCH 			= "Facing issues in verifying the Old run details. ";
	public static String ERR_NEW_RUN_INITIATION 	= "Facing issues in initiating a new run. ";
	public static String ERR_MODULE_MGMT_INSERT		= "Facing issues in inserting data in GDPR Output table - RUN_MODULE_MGMT for the module. ";
	public static String ERR_MODULE_MGMT_FETCH		= "Facing issues in fetching data from GDPR Output table - RUN_MODULE_MGMT. ";
			
	public static String MSG_OLD_RUN_FETCHED		= "Old run detail fetched. Run Id : ";
	public static String MSG_NEW_RUN_INITIATED		= "New run initiated. Run Id : ";
	
	public static String MSG_FILE_ROWS_PROCESSED_0	= "No new rows are added to Anonymization file for processing.";
	public static String MSG_FILE_ROWS_PROCESSED	= "Rows parsed from Anonymization file : ";
	public static String MSG_IMPACT_FIELD_ROWS		= "New rows inserted in IMPACT_FIELD table : ";
	public static String MSG_ANONYMIZATION_DTL_ROWS = "New rows inserted in ANONYMIZATION_DETAIL table : ";
	public static String MSG_SUMMARY_ROWS			= "Summary details : ";
	public static String MSG_REORGANIZEINPUT_JOB	= "Reorganizing GDPR_Depersonalization__c Job initiated. ";
	public static String MSG_MODULE_STATUS_INSERT	= "The status details is loaded successfully for the following : ";
	public static String MSG_REORGANIZEINPUT		= "The data GDPR_Depersonalization__c has been reorganized";
	public static String MSG_TAGGING_DATA			= "Tagged the data for SF_ARCHIVE table ";
	public static String MSG_ANONYMIZE_DATA			= "Anonymize the data for SF_ARCHIVE table ";
	
	public static String MSG_TAGGING_JOB			= "Job to Tag Archival tables initiated";
	public static String MSG_DEPERSONALIZATION_JOB  = "Job to Depersonalize archival tables initiated. ";
	
	public static String ERR_FILE_EMPTY						= "The file is empty. Please upload file with content. ";
	public static String ERR_PARSE_ANONYMIZATION_IO			= "Facing  IO issues in parsing the Anonymization file. ";
	public static String ERR_PARSE_ANONYMIZATION			= "Facing  issues in parsing the Anonymization file. ";
	public static String ERR_GDPR_INPUT_FETCH				= "Facing issues in accessing GDPR Input tables - IMPACT_TABLE, IMPACT_FIELD. ";
	public static String ERR_IMPACT_FIELD_INSERT			= "Facing issues in inserting data in GDPR Input table - IMPACT_FIELD. ";
	public static String ERR_GDPR_INPUT_ALL_FETCH			= "Facing issues in accessing GDPR Input tables - CATEGORY, IMPACT_TABLE, IMPACT_FIELD,"
																+ " ANONYMIZATION_DETAIL. ";
	public static String ERR_ANONYMIZATION_DETAIL_INSERT	= "Facing issues in inserting data in GDPR Input table - ANONYMIZATION_DETAIL. ";
	
	public static String ERR_RUN_ERROR_MGMT_INSERT			= "Facing issues in inserting data in GDPR Output table - RUN_ERROR_MGMT. ";
	public static String ERR_RUN_MODULE_MGMT_INSERT			= "Facing issues in inserting data in GDPR Output table - RUN_MODULE_MGMT. ";
	
	// Initiatlization Status
	public static String RUN_ANONYMIZATION_INSERT = "Run Anonymization Initiated Count : ";
	public static String RUN_ANONYMIZATION_ZERO   = "No Run Anonymization rows to process. ";
	public static String ERR_FETCH_COUNTRY_DETAIL = "Facing issues in fetching Country details ";
	public static String ERR_FETCH_TABLE_DETAIL	= "Facing issues in fetching Impact Table details";
	public static String ERR_RUN_ANONYMIZATION_LOAD   = "Facing issues in loading table - RUN_ANONYMIZATION. ";
	
	public static String ERR_RUN_SUMMARY_DATA_FETCH     = "Facing issues in fetching the Summary data from the INPUT Tables. ";
	public static String ERR_RUN_SUMMARY_DATA_TRANSFORM = "Facing issues in transforming the Summary data from the INPUT Tables. ";
	public static String ERR_RUN_SUMMARY_DATA_LOAD		= "Facing issues in loading the Summary data from the INPUT Tables. ";
	public static String ERR_RUN_SUMMARY_MGMT_INSERT  = "Facing issues in inserting data in GDPR Output Table - RUN_SUMMARY_MGMT. ";
	
	/*****Backup Table Processor Code Change Starts***/
	public static String ERR_RUN_BACKUP_TABLE_PROCESSOR   = "Facing issues in backup table processor - BackupTableProcessor. ";
	public static String ERR_RUN_BACKUP_TABLE_COLUMNCHECK   = "Facing issues in checking backup table column exists or not - BackupTableProcessor. ";
	public static String ERR_RUN_BACKUP_TABLE_REFRESH   = "Facing issues in refreshing backup tables- BackupTableProcessor. ";
	
	public static String ERR_RUN_GDPR_DEPERSONALIZATION	= "Facing issues while processing the GDPR Depersonalization table data. ";
	public static String ERR_REORGANIZE_JOB_RUN	= "Facing issues while initiating reorganizing job. ";
	public static String ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER   = "Facing issues in refreshing backup tables using scheduler- BackupTableProcessor. ";
	public static String ERR_PREV_JOB_STATUS_CHECK = "Facing issues while verifying the status of Previous job. ";
	public static String ERR_TAGGED_JOB_RUN	= "Facing issues while initiating Tagged job. ";
	public static String ERR_DEPERSONALIZE_JOB_RUN = "Facing issues while initiating depersonlization job. ";
	public static String ERR_REORGANIZE_JOB_PROCESS = "Facing issues while building job. ";
	public static String ERR_TAG_JOB_PROCESS = "Facing issues while building job. ";
	public static String ERR_ANONYMIZE_JOB_RPOCESS = "Facing issues while building job. ";
		
	/*****Backup Table Processor Code Change Ends***/	 	
	public static String ERR_REORGANIZEINPUT_BEFORESTEP	= "Facing issues in processing the Job parameters. ";
	public static String ERR_REORGANIZEINPUT_DATA_LOAD	= "Facing issues in inserting data in GDPR Output Table - DATA_LOAD. ";
	public static String ERR_GDPR_DEPERSONALIZATION_LOAD = "Facing issues in processing data from GDPR_DEPERSONALIZATION__C. ";
	public static String ERR_TAGGING_LOAD = "Facing issues in tagging Sf_archive tables data. ";
		
	//BackupService
	public static String MODULE_BACKUPSERVICE 				= "BackupService Module";
	public static String JOB_BACKUP_SERVICE_LISTENER = "Backup Service Listener";
	public static String JOB_BACKUP_SERVICE_INPUT_RUNID		 = "RunId";
	public static String JOB_BACKUP_SERVICE_INPUT_JOBID		 = "JobId";	

	public static String MSG_BACKUPSERVICE_JOB	= "Backup Service Job initiated. ";
	public static String ERR_BACKUPSERVICE_JOB_RUN	= "Facing issues while initiating Backup Service job. ";

	public static String SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE = "Backup Service Job Sub Module"; 
	public static String SUB_MODULE_BACKUPSERVICE_DATA			= "Backup Service data Batch Sub Module";
	public static String MSG_BACKUPSERVICE_INPUT		= "The Backup Data has been completed";
	public static String ERR_DATABACKUP_PROCESS     = "Facing issues in inserting backup data in backup tables. ";
	public static String ERR_BACKUPSERVICE_DATA_COUNT	= "Facing issues in Updating backup Count. ";
	public static String ERR_BACKUPSERVICE_JOB = "Facing issues inititing backup service job. ";
	public static String ERR_BACKUPSERVICE_STEP = "Facing issues inititing backup service step. ";
}