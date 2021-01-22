package com.amazon.gdpr.batch;

import java.util.Date;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.DepersonalizationProcessor;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class TaggingJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;	
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_TAGGINGCOMPLETIONLISTENER;
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
	Date moduleEndDateTime = null;
	String failureStatus = null;
	Boolean exceptionOccured = false;
	
	
	public TaggingJobCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		JobParameters jobParameters = jobExecution.getJobParameters();
		
		long runId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
		/*String tableName = jobParameters.getString(GlobalConstants.JOB_INPUT_TABLE_NAME);
		String countryCode = jobParameters.getString(GlobalConstants.JOB_INPUT_COUNTRY_CODE);
		int categoryId = Integer.parseInt(jobParameters.getString(GlobalConstants.JOB_INPUT_CATEGORY_ID));*/
		
		moduleStartDateTime = jobExecution.getStartTime();
		moduleEndDateTime = jobExecution.getEndTime();			
		String moduleStatus = "";
		String jobExitStatus = jobExecution.getExitStatus().getExitCode();
		String errorMessage = jobExecution.getAllFailureExceptions().toString();
		
		String batchJobStatus = "BATCH JOB COMPLETED SUCCESSFULLY for runId - "+runId+" for runSummaryId - "+ runSummaryId
				+" with status -"+jobExitStatus;
		if (jobExitStatus.equalsIgnoreCase(ExitStatus.COMPLETED.getExitCode()) || jobExitStatus.equalsIgnoreCase(ExitStatus.FAILED.getExitCode()) ) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobRelatedName+ " "+batchJobStatus);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobExecution.getExitStatus().getExitCode());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobExecution.getExitStatus().getExitCode().toString());
			try {
				switch (jobExecution.getExitStatus().getExitCode().toString()) {					
					case "COMPLETED" :
						moduleStatus = GlobalConstants.STATUS_SUCCESS;
					case "FAILED" : 
						moduleStatus = GlobalConstants.STATUS_FAILURE;
					default :
						moduleStatus = GlobalConstants.STATUS_SUCCESS;
				}
				String taggingData = GlobalConstants.MSG_TAGGING_DATA + "for runId - "+runId+" for runSummaryId - "+ runSummaryId;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, GlobalConstants.SUB_MODULE_TAGGED_DATA,
						moduleStatus, moduleStartDateTime, moduleEndDateTime, taggingData, errorMessage);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				/*if(GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(moduleStatus)) {
					depersonalizationProcessor.depersonalizationInitialize(runId, runSummaryId);
				}*/
			} catch(GdprException exception) {
				exceptionOccured = true;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
			}
		}		
	}
}