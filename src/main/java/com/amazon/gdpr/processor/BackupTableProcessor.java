package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.BackupTableProcessorDaoImpl;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.input.ImpactTableDetails;
import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Processor ensures that the Backup table structure is updated, backup
 * table is refreshed before run and loaded before depersonalization
 ****************************************************************************************/
@Component
public class BackupTableProcessor {
	private static String CURRENT_CLASS = GlobalConstants.CLS_BACKUPTABLEPROCESSOR;

	@Autowired
	private BackupTableProcessorDaoImpl backupTableProcessorDaoImpl;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;

	public String processBkpupTable(long runId) throws GdprException {
		String CURRENT_METHOD = "processBkpupTable";
		Boolean bkpupTblProcessStatus = false;
		RunErrorMgmt runErrorMgmt = null;
		List<BackupTableDetails> lstBackupTableDetails = backupTableProcessorDaoImpl.fetchBackupTableDetails();
		List<ImpactTableDetails> lstImpactTableDetails = gdprInputDaoImpl.fetchImpactTableDetailsMap();
		String errorDetails = "";
		
		try {
			if (refreshBackupTables(lstBackupTableDetails)) {
				bkpupTblProcessStatus = bkpupTableCheck(lstBackupTableDetails, lstImpactTableDetails);
			}
			if (!bkpupTblProcessStatus) {
				// load ModuleMgmt
				// load ErrorMgmt
			}
		} catch (Exception exception) {
			System.out.println(
					CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_RUN_BACKUP_TABLE_PROCESSOR);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD,
					GlobalConstants.ERR_RUN_BACKUP_TABLE_PROCESSOR, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_PROCESSOR + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(
					GlobalConstants.ERR_RUN_BACKUP_TABLE_PROCESSOR + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return "";
	}

	/*
	 * Refresh Backup Tables
	 */
	public Boolean refreshBackupTables(List<BackupTableDetails> lstBackupTableDetails) throws GdprException {
		String CURRENT_METHOD = "refreshBackupTables";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		Boolean refreshBkpupTableStatus = false;
		// Fetch the Backup Table name from ImpactTable and truncate the tables
		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try {
			refreshBkpupTableStatus = backupTableProcessorDaoImpl.refreshBackupTables(lstBackupTableDetails);
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESH);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD,
					GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESH, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESH + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(
					GlobalConstants.ERR_RUN_BACKUP_TABLE_REFRESH + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT,  errorDetails);
		}
		return refreshBkpupTableStatus;
	}

	public Boolean bkpupTableCheck(List<BackupTableDetails> lstBackupTableDetails,
			List<ImpactTableDetails> lstImpactTableDetails) throws GdprException {
		String CURRENT_METHOD = "bkpupTableCheck";
		Boolean bkpupTableCheckStatus = false;
		String impactTableName = null;
		String impactColumnName = null;
		String impactColumnType = null;
		String backupTableName = null;
		String backupColumnName = null;
		boolean alterAppl = false;
		boolean alterAssmt = false;
		boolean alterEmmsg = false;
		boolean alterErrLog = false;
		boolean alterIntTrans = false;
		boolean alterIntrv = false;
		boolean alterNote = false;
		boolean alterResponse = false;
		boolean alterResponseAns = false;
		boolean alterTask = false;
		boolean alterUser = false;
		boolean alterAtt = false;

		String stAppl = "";
		String stAssmt = "";
		String stEmmsg = "";
		String stErrLog = "";
		String stIntTrans = "";
		String stIntrv = "";
		String stNote = "";
		String stResponse = "";
		String stResponseAns = "";
		String stTask = "";
		String stUser = "";
		String stAtt = "";
        List<String> altQury=new ArrayList<String>();
		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try {
			for (ImpactTableDetails impactTableDtls : lstImpactTableDetails) {
				Boolean clExistStatus = false;
				impactTableName = impactTableDtls.getImpactTableName().toUpperCase();
				impactColumnName = impactTableDtls.getImpactColumnName().toUpperCase();
				impactColumnType = impactTableDtls.getImpactColumnType().toUpperCase();
				for (BackupTableDetails backupTableDtls : lstBackupTableDetails) {
					backupTableName = backupTableDtls.getBackupTableName().toUpperCase().replaceAll("BKP_", "");
					backupColumnName = backupTableDtls.getBackupTablecolumn().toUpperCase();
					if (impactTableName.equalsIgnoreCase(backupTableName)
							&& impactColumnName.equalsIgnoreCase(backupColumnName)) {
						clExistStatus = true;

					}

				}
				if (!clExistStatus) {
					switch (impactTableName) {
					case "APPLICATION__C":
						alterAppl = true;
						stAppl = stAppl + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "ASSESSMENT__C":
						alterAssmt = true;
						stAssmt = stAssmt + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "EMAILMESSAGE":
						stEmmsg = stEmmsg + " ADD COLUMN" + impactColumnName + " " + impactColumnType + ",";
						alterEmmsg = true;
						break;
					case "ERROR_LOG__C":
						alterErrLog = true;
						stErrLog = stErrLog + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "INTEGRATION_TRANSACTION__C":
						alterIntTrans = true;
						stIntTrans = stIntTrans + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "INTERVIEW__C":
						alterIntrv = true;
						stIntrv = stIntrv + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "NOTE":
						alterNote = true;
						stNote = stNote + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "RESPONSE__C":
						alterResponse = true;
						stResponse = stResponse + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "RESPONSE_ANSWER__C":
						alterResponseAns = true;
						stResponseAns = stResponseAns + " ADD COLUMN " + impactColumnName + " " + impactColumnType
								+ ",";
						break;
					case "TASK":
						alterTask = true;
						stTask = stTask + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "USER":
						alterUser = true;
						stUser = stUser + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					case "ATTACHMENT":
						alterAtt = true;
						stAtt = stAtt + " ADD COLUMN " + impactColumnName + " " + impactColumnType + ",";
						break;
					default:

					}
				}
			}
			if (alterAppl) {
				stAppl = StringUtils.chop(stAppl);
				String query = "ALTER TABLE GDPR.BKP_APPLICATION__C" + stAppl + ";";
				altQury.add(query);
			}
			if (alterAssmt) {
				stAssmt = StringUtils.chop(stAssmt);
				String query = "ALTER TABLE GDPR.BKP_ASSESSMENT__C" + stAssmt + ";";
				altQury.add(query);
			}
			if (alterEmmsg) {
				stEmmsg = StringUtils.chop(stEmmsg);
				String query = "ALTER TABLE GDPR.BKP_EMAILMESSAGE" + stEmmsg + ";";
				altQury.add(query);
			}
			if (alterErrLog) {
				stErrLog = StringUtils.chop(stErrLog);
				String query = "ALTER TABLE GDPR.BKP_ERROR_LOG__C" + stErrLog + ";";
				altQury.add(query);
			}
			if (alterIntTrans) {
				stIntTrans = StringUtils.chop(stIntTrans);
				String query = "ALTER TABLE GDPR.BKP_INTEGRATION_TRANSACTION__C" + stIntTrans + ";";
				altQury.add(query);
			}
			if (alterIntrv) {
				stIntrv = StringUtils.chop(stIntrv);
				String query = "ALTER TABLE GDPR.BKP_INTERVIEW__C" + stIntrv + ";";
				altQury.add(query);
			}
			if (alterNote) {
				stNote = StringUtils.chop(stNote);
				String query = "ALTER TABLE GDPR.BKP_ASSESSMENT__C" + stNote + ";";
				altQury.add(query);
			}
			if (alterResponse) {
				stResponse = StringUtils.chop(stResponse);
				String query = "ALTER TABLE GDPR.BKP_RESPONSE__C" + stResponse + ";";
				altQury.add(query);
			}
			if (alterResponseAns) {
				stResponseAns = StringUtils.chop(stResponseAns);
				String query = "ALTER TABLE GDPR.BKP_RESPONSE_ANSWER__C" + stResponseAns + ";";
				altQury.add(query);
			}
			if (alterTask) {
				stTask = StringUtils.chop(stTask);
				String query = "ALTER TABLE GDPR.BKP_TASK" + stTask + ";";
				altQury.add(query);
			}
			if (alterUser) {
				stUser = StringUtils.chop(stUser);
				String query = "ALTER TABLE GDPR.BKP_USER" + stUser + ";";
				altQury.add(query);
			}
			if (alterAtt) {
				stAtt = StringUtils.chop(stAtt);
				String query = "ALTER TABLE GDPR.BKP_ATTACHMENT" + stAtt + ";";
				altQury.add(query);
			}
			System.out.println("altQury altQury : "+altQury);
			String[] altQryArr = new String[altQury.size()];
			altQryArr = altQury.toArray(altQryArr);
			
			System.out.println("altQryArr altQryArr : "+altQryArr);
			if (altQury != null && !altQury.isEmpty()) {

			bkpupTableCheckStatus=backupTableProcessorDaoImpl.alterBackupTable(altQryArr);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_COLUMNCHECK);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD,
					GlobalConstants.ERR_RUN_BACKUP_TABLE_COLUMNCHECK, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
					+ GlobalConstants.ERR_RUN_BACKUP_TABLE_COLUMNCHECK + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_RUN_BACKUP_TABLE_COLUMNCHECK + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return bkpupTableCheckStatus;
	}
}