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

import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.RunMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service will reorganize the GDPR_Depersonalization__c table  
 * This will be invoked by the GDPRController
 ****************************************************************************************/
@Component
public class ReOrganizeInputProcessor {

	public static String CURRENT_CLASS	= GlobalConstants.CLS_REORGANIZE_INPUT_PROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processreorganizeInputJob;
	
	@Autowired
	RunMgmtProcessor runMgmtProcessor;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
			
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
			
	public String reOrganizeData(long runId, List<String> selectedCountries) throws GdprException {
		String CURRENT_METHOD = "reOrganizeData";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
		JobThread jobThread = new JobThread(runId);
		jobThread.start();
		return GlobalConstants.MSG_REORGANIZEINPUT_JOB;
	}
	
	class JobThread extends Thread {
		long runId;
		JobThread(long runId){
			this.runId = runId;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String reOrganizeDataStatus = "";
			Boolean exceptionOccured = false;
			Date moduleStartDateTime = null;
			Date moduleEndDateTime = null;
			Date gdprDataFetchdate = null;
			String errorDetails = "";

			try {
	    		moduleStartDateTime = new Date();
	    		gdprDataFetchdate = new Date();
	    				    		
				JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
				jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);
				// TODO Pass Country code 
				//jobParameterBuilder.addString(key, parameter);
				jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
				
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
				JobParameters jobParameters = jobParameterBuilder.toJobParameters();

				jobLauncher.run(processreorganizeInputJob, jobParameters);
				reOrganizeDataStatus = GlobalConstants.MSG_REORGANIZEINPUT_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = GlobalConstants.ERR_REORGANIZE_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			} 
	    	try {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				moduleEndDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						moduleEndDateTime, reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				
			} catch(GdprException exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			}
		}
	}
}