package com.amazon.gdpr.batch;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class BackupJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	private static String CURRENT_CLASS		 		= "BackupJobCompletionListener";
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
	Date moduleEndDateTime = null;
	String failureStatus = null;
	Boolean exceptionOccured = false;
	
	public BackupJobCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobRelatedName+ " BATCH JOB COMPLETED SUCCESSFULLY");
			JobParameters jobParameters = jobExecution.getJobParameters();
			
			long runId = jobParameters.getLong(GlobalConstants.JOB_REORGANIZE_INPUT_RUNID);
			moduleStartDateTime = jobExecution.getStartTime();
			moduleEndDateTime = jobExecution.getEndTime();			
			failureStatus = jobExecution.getFailureExceptions().toString();		
			String errorMessage = jobExecution.getAllFailureExceptions().toString();
			
			try {
				String moduleStatus = (failureStatus != null && failureStatus.equalsIgnoreCase("")) ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, GlobalConstants.SUB_MODULE_BACKUPSERVICE_DATA,
						moduleStatus, moduleStartDateTime, moduleEndDateTime, GlobalConstants.MSG_BACKUPSERVICE_INPUT, errorMessage);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			} catch(GdprException exception) {
				exceptionOccured = true;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
			}
		}		
	}
}