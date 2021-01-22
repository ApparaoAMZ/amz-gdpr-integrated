package com.amazon.gdpr.scheduler;

import java.time.LocalTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.BackupTableProcessorDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.processor.BackupTableProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Sheduler is used to schedule the backupdata refresh for every week
 * tracker.
 ****************************************************************************************/
@Component
public class ScheduledTasks {

	public static String CURRENT_CLASS = "ScheduledTasks";

	@Autowired
	BackupTableProcessor backupTableProcessor;

	@Autowired
	private BackupTableProcessorDaoImpl backupTableProcessorDaoImpl;

	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;

	/**
	 * This method navigates the list of BackupTables
	 * RUN_ANONYMIZATION_MAPPING table
	 * 
	 * @param 
	 * @return
	 */
	@Scheduled(cron = "${cron.expression}")
	public void refreshBackupTableScheduler() throws GdprException {
		String CURRENT_METHOD = "refreshBackupTableScheduler";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");

		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try {
			System.out.println("start of call BackupTable Refresh Scheduler:: " + LocalTime.now());
			List<BackupTableDetails> lstBackupTableDetails = backupTableProcessorDaoImpl.fetchBackupTableDetails();
			Boolean refreshStatus = backupTableProcessor.refreshBackupTables(lstBackupTableDetails);
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD,
					GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER
					+ GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(
					GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESHSCHEDULER + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
	}
}