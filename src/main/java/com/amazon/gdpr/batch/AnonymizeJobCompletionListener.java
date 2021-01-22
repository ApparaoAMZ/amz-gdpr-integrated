package com.amazon.gdpr.batch;

import java.util.Date;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class AnonymizeJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZECOMPLETIONLISTENER;
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
	Date moduleEndDateTime = null;
	String failureStatus = null;
	Boolean exceptionOccured = false;
		
	public AnonymizeJobCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		JobParameters jobParameters = jobExecution.getJobParameters();
		
		long runId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
		
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
				String anonymizeData = GlobalConstants.MSG_ANONYMIZE_DATA + "for runId - "+runId+" for runSummaryId - "+ runSummaryId;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, moduleStatus, moduleStartDateTime, moduleEndDateTime, 
						anonymizeData, errorMessage);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			} catch(GdprException exception) {
				exceptionOccured = true;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
			}
		}
	}
}