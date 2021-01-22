package com.amazon.gdpr.service;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.RunMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service performs the Depersonalization activity on the Heroku Backup
 * Data This will be invoked by the GDPRController
 ****************************************************************************************/
@Service
public class BackupService {
	public static String CURRENT_CLASS	= GlobalConstants.CLS_BACKUPSERVICE;
	public static String MODULE_DATABACKUP = GlobalConstants.MODULE_DATABACKUP;
	public static String STATUS_SUCCESS = GlobalConstants.STATUS_SUCCESS;

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job processGdprBackupServiceJob;

	@Autowired
	RunMgmtProcessor runMgmtProcessor;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;

	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	public String backupServiceInitiate(long runId) {
		String CURRENT_METHOD = "backupServiceInitiate";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
		BackupJobThread bkpJobThread = new BackupJobThread(runId);
		bkpJobThread.start();
		return GlobalConstants.MSG_BACKUPSERVICE_JOB;
	}

	/**
	 * The main method which initiates the Backup Service
	 * 
	 * @param runId             The current run's reference
	 * @param mapRunSummaryMgmt The overall summary of the Depersonalization is
	 *                          managed in this Table
	 * @return Updated Summary information with the backup detail loaded
	 */
	public String backupService(long runId) {
		String CURRENT_METHOD = "backupService";
		System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + ":: Inside method");
		List<RunModuleMgmt> lstRunModuleMgmt = gdprOutputDaoImpl.fetchLastModuleData(runId);
		Map<String, String> mpaModuleData = lstRunModuleMgmt.stream()
				.collect(Collectors.toMap(RunModuleMgmt::getSubModuleName, RunModuleMgmt::getModuleStatus,
						(oldValue, newValue) -> newValue, LinkedHashMap::new));
		System.out.println("mpaModuleData:::::" + mpaModuleData);
		String jobStatus = mpaModuleData.get(GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE);
		String batchModulestatus = mpaModuleData.get(GlobalConstants.SUB_MODULE_REORGANIZE_DATA);
		String summaryModulestatus = mpaModuleData.get(GlobalConstants.SUB_MODULE_SUMMARY_DATA_INITIALIZE);
		System.out.println("batchModulestatus:::::" + batchModulestatus);
		boolean jobrunflag = false;
		if (summaryModulestatus != null && summaryModulestatus.equalsIgnoreCase(GlobalConstants.STATUS_SUCCESS)) {
			while (true) {
				if (jobStatus != null && jobStatus.equalsIgnoreCase(GlobalConstants.STATUS_SUCCESS)
						&& batchModulestatus != null
						&& batchModulestatus.equalsIgnoreCase(GlobalConstants.STATUS_SUCCESS)) {
					jobrunflag = true;
					break;
				} else {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					lstRunModuleMgmt = gdprOutputDaoImpl.fetchLastModuleData(runId);
					mpaModuleData = lstRunModuleMgmt.stream().collect(Collectors.toMap(RunModuleMgmt::getSubModuleName,
							RunModuleMgmt::getModuleStatus, (oldValue, newValue) -> newValue, LinkedHashMap::new));
					jobStatus = mpaModuleData.get(GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE);
					batchModulestatus = mpaModuleData.get(GlobalConstants.SUB_MODULE_REORGANIZE_DATA);
					System.out.println("batchModulestatus2:::::" + batchModulestatus);

					if (jobStatus != null && jobStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) {
						break;
					}
				}

				System.out.println("jobrunflag::::" + jobrunflag);
			}

			if (jobrunflag) {
				BackupJobThread bkpJobThread = new BackupJobThread(runId);
				bkpJobThread.start();
			}
		}
		return GlobalConstants.MSG_BACKUPSERVICE_JOB;
	}

	class BackupJobThread extends Thread {
		long runId;

		BackupJobThread(long runId) {
			this.runId = runId;
		}

		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String backupServiceStatus = "";
			Boolean exceptionOccured = false;
			Date moduleStartDateTime = null;
			Date moduleEndDateTime = null;
			Date gdprDataFetchdate = null;

			try {
				moduleStartDateTime = new Date();
				gdprDataFetchdate = new Date();

				JobParametersBuilder jobParameterBuilder = new JobParametersBuilder();
				jobParameterBuilder.addLong(GlobalConstants.JOB_BACKUP_SERVICE_INPUT_RUNID, runId);
				jobParameterBuilder.addLong(GlobalConstants.JOB_BACKUP_SERVICE_INPUT_JOBID, new Date().getTime());

				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: JobParameters set ");
				JobParameters jobParameters = jobParameterBuilder.toJobParameters();

				jobLauncher.run(processGdprBackupServiceJob, jobParameters);
				backupServiceStatus = GlobalConstants.MSG_BACKUPSERVICE_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
					| JobParametersInvalidException exception) {
				exceptionOccured = true;
				backupServiceStatus = GlobalConstants.ERR_BACKUPSERVICE_JOB_RUN;
				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: " + backupServiceStatus);
				exception.printStackTrace();
			}
			try {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
						: GlobalConstants.STATUS_SUCCESS;
				moduleEndDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
						GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
						moduleEndDateTime, backupServiceStatus);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				
			} catch (GdprException exception) {
				exceptionOccured = true;
				backupServiceStatus = backupServiceStatus + exception.getExceptionMessage();
				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: " + backupServiceStatus);
			}
		}
	}

	/**
	 * The list of all the parent Ids for each of the table is loaded in a map
	 * 
	 * @return Returns the parentIds mapped to their respective tablename
	 */
	public Map<String, Set<String>> fetchParentIds() {
		Map<String, Set<String>> mapParentIds = new HashMap<String, Set<String>>();
		// Fetch Impact Table Id's parent table and Parent table Key and fetch
		// respective table with condition
		// load it in a Set
		// Fetch the parent ids from respective table and load it in the map
		return mapParentIds;
	}

	/**
	 * Backup activity is performed and the details are loaded into the
	 * RunSummaryMgmt table
	 * 
	 * @param mapParentIds      The parent ids are passed as conditions in the table
	 * @param mapRunSummaryMgmt The current summary information is passed on
	 * @return The updated summary information with the back up details
	 */
	public Map<String, RunSummaryMgmt> performBackup(Map<String, Set<String>> mapParentIds,
			Map<String, RunSummaryMgmt> mapRunSummaryMgmt) {
		Map<String, RunSummaryMgmt> mapRunSummaryMgmtUpdated = null;
		// Navigate the RunSummaryMgmt map to fetch the Backup queries one by one
		// With the parent ids as the criteria, fetch the records and load it into
		// respective tables
		// Add the counts in RunSummaryMgmt table
		return mapRunSummaryMgmtUpdated;
	}
}