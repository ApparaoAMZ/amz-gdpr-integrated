package com.amazon.gdpr.service;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazon.gdpr.model.gdpr.output.RunAnonymization;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.AnonymizationFileProcessor;
import com.amazon.gdpr.processor.BackupTableProcessor;
import com.amazon.gdpr.processor.ReOrganizeInputProcessor;
import com.amazon.gdpr.processor.RunMgmtProcessor;
import com.amazon.gdpr.processor.SummaryDataProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.GdprInput;

/****************************************************************************************
 * This Service performs the Initialization activity on the Heroku GDPR  
 * This will be invoked by the GDPRController
 ****************************************************************************************/
@Service
public class InitService {
	
	public static String CURRENT_CLASS	= GlobalConstants.MODULE_INITIALIZATION;
	public static String STATUS_SUCCESS = GlobalConstants.STATUS_SUCCESS;
	
	public List<RunAnonymization> lstRunAnonymization=null;
	public Map<String, RunSummaryMgmt> mapRunSummaryMgmt=null;
	private long runId = 0;
	
	@Autowired
	RunMgmtProcessor runMgmtProcessor;
	
	@Autowired
	AnonymizationFileProcessor anonymizationFileProcessor;

	@Autowired
	BackupTableProcessor bkpupTableProcessor;
	
	@Autowired
	ReOrganizeInputProcessor reOrganizeInputProcessor;

	@Autowired
	SummaryDataProcessor summaryDataProcessor;
	
	//@Autowired
	//BackupService backupService;
	/**
	 * This method initiates the InitService activities
	 * @param runName
	 * @return
	 */
	public String initService(String runName, List<String> selectedCountries) {
		String CURRENT_METHOD = "initService";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		String initServiceReturnStatus = "";
		Boolean exceptionOccured = false;
				
		try{
			//Initiates the run. Establishes the run in the DB
			runId =  runMgmtProcessor.initializeRun(runName);
			initServiceReturnStatus = runMgmtProcessor.initializeRunStatus;
			String[] initServiceStatus = initialize(runId, selectedCountries);
			initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.SEMICOLON_STRING + initServiceStatus[1];
		} catch(GdprException exception) {
			exceptionOccured = true;
			initServiceReturnStatus = initServiceReturnStatus + exception.getExceptionMessage();
		}
		
		if (exceptionOccured)
			initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.ERR_DISPLAY;
		return initServiceReturnStatus;
	}
	
	/**
	 * The initialization activities are performed. Verification of all the basic steps required for the project
	 * @param runId
	 * @return
	 */
	public String[] initialize(long runId, List<String> selectedCountries) throws GdprException {
		
		String CURRENT_METHOD = "initialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		String[] initializationStatus = new String[2];		
		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Anonymization Processor : "+LocalTime.now());
		int insertRunAnonymizationCounts = anonymizationFileProcessor.loadRunAnonymization(runId, selectedCountries);		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Anonymization Processor : "+LocalTime.now());
		if(insertRunAnonymizationCounts == 0) {
			initializationStatus[0] = GlobalConstants.STATUS_FAILURE;
			initializationStatus[1] = GlobalConstants.RUN_ANONYMIZATION_ZERO;
		}
		else {
			initializationStatus[0] = GlobalConstants.STATUS_SUCCESS;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Backup Processor : "+LocalTime.now());
			String processBackupTableStatus = bkpupTableProcessor.processBkpupTable(runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Backup Processor : "+LocalTime.now());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Summary Processor : "+LocalTime.now());			
			String summaryStatus = summaryDataProcessor.processSummaryData(runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Summary Processor : "+LocalTime.now());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before GDPR Data Processor : "+LocalTime.now());
			String reOrganizeDataStatus = reOrganizeInputProcessor.reOrganizeData(runId, selectedCountries);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After GDPR Data Processor : "+LocalTime.now());			
						
			initializationStatus[1] = GlobalConstants.RUN_ANONYMIZATION_INSERT + insertRunAnonymizationCounts +
					GlobalConstants.SEMICOLON_STRING + processBackupTableStatus + GlobalConstants.SEMICOLON_STRING + summaryStatus + 
					GlobalConstants.SEMICOLON_STRING + reOrganizeDataStatus;
		}
		return initializationStatus;
	}
	
	/**
	 * @return the runId
	 */
	public long getRunId() {
		return runId;
	}

	/**
	 * @param runId the runId to set
	 */
	public void setRunId(long runId) {
		this.runId = runId;
	}
		
	public GdprInput loadGdprForm() {
		GdprInput gdprInput = new GdprInput();
		
		try{
			gdprInput = runMgmtProcessor.loadCountryDetail();
		} catch(GdprException exception) {
			gdprInput.setErrorStatus(GlobalConstants.STATUS_FAILURE);
			gdprInput.setRunStatus(GlobalConstants.ERR_LOAD_FORM);
		}
		return gdprInput;
	}
}