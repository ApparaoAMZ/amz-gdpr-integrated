package com.amazon.gdpr.processor;

import java.util.Date;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service will initiate the DepersonalizationBatchConfig Job  
 * This will be invoked after the completion of Tagging 
 ****************************************************************************************/
@Component
public class DepersonalizationProcessor {
public static String CURRENT_CLASS	= GlobalConstants.CLS_DEPERSONALIZATIONPROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processAnonymizeJob;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	Date moduleStartDateTime = null;
	
	public void depersonalizationInitialize(long runId) throws GdprException {
		String CURRENT_METHOD = "depersonalizationInitialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
				
		DepersonalizationJobThread jobThread = new DepersonalizationJobThread(runId);
		jobThread.start();
	}
	
	class DepersonalizationJobThread extends Thread {
		
		long runId;
		
		DepersonalizationJobThread(long runId){
			this.runId = runId;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String depersonalizationDataStatus = "";
			Boolean exceptionOccured = false;			
			Date moduleEndDateTime = null;			
			String errorDetails = "";

			try {
				moduleStartDateTime = new Date();
				
				List<RunSummaryMgmt> lstRunSummaryMgmt = gdprOutputDaoImpl.fetchRunSummaryDetail(runId);
				if(lstRunSummaryMgmt != null) {
					for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) { 
						JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);						
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID, runSummaryMgmt.getSummaryId());
						
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
						JobParameters jobParameters = jobParameterBuilder.toJobParameters();
						jobLauncher.run(processAnonymizeJob, jobParameters);						
					}
				}				
				depersonalizationDataStatus = GlobalConstants.MSG_DEPERSONALIZATION_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = GlobalConstants.ERR_DEPERSONALIZE_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			} 
	    	try {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				moduleEndDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						moduleEndDateTime, depersonalizationDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			} catch(GdprException exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = depersonalizationDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
			}
		}
	}
}