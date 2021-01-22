package com.amazon.gdpr.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazon.gdpr.batch.BackupJobCompletionListener;
import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.BackupServiceInput;
import com.amazon.gdpr.model.BackupServiceOutput;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the Reading of GDPR.RUN_SUMMARY_MGMT table and
 * Writing into GDPR.BKP Tables
 ****************************************************************************************/
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class })
@Configuration
public class GdprBackupServiceBatchConfig {
	private static String CURRENT_CLASS = "GdprBackupServiceBatchConfig";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;

	@Autowired
	public BackupServiceDaoImpl backupServiceDaoImpl;
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;

	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	public long runId;

	@Bean
	@StepScope
	public JdbcCursorItemReader<BackupServiceInput> backupServiceReader(@Value("#{jobParameters[RunId]}") long runId)
			throws GdprException {
		String gdprSummaryDataFetch = SqlQueriesConstant.GDPR_SUMMARYDATA_FETCH;
		String CURRENT_METHOD = "BackupreaderClass";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. " + runId);
		JdbcCursorItemReader<BackupServiceInput> reader = null;
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		String errorDetails = "";

		try {
			reader = new JdbcCursorItemReader<BackupServiceInput>();
			reader.setDataSource(dataSource);
			reader.setSql(gdprSummaryDataFetch + runId);
			reader.setRowMapper(new BackupServiceInputRowMapper());

		} catch (Exception exception) {
			exceptionOccured = true;
			backupDataStatus = "Facing issues in reading summary management table. ";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if (exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
						: GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
						GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
						moduleStartDateTime, backupDataStatus);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch (GdprException exception) {
			backupDataStatus = backupDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(backupDataStatus, errorDetails);
		}
		return reader;
	}

	// To set values into BackupServiceInput Object
	public class BackupServiceInputRowMapper implements RowMapper<BackupServiceInput> {
		private String CURRENT_CLASS = "BackupServiceInputRowMapper";

		@Override
		public BackupServiceInput mapRow(ResultSet rs, int rowNum) throws SQLException {
			// TODO Auto-generated method stub
			String CURRENT_METHOD = "mapRow";

			String runId = rs.getString("RUN_ID");
			// System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside
			// method. " + rs.getInt("IMPACT_TABLE_ID"));
			return new BackupServiceInput(rs.getLong("SUMMARY_ID"), rs.getLong("RUN_ID"), rs.getInt("CATEGORY_ID"),
					rs.getString("REGION"), rs.getString("COUNTRY_CODE"), rs.getInt("IMPACT_TABLE_ID"),
					rs.getString("BACKUP_QUERY"), rs.getString("DEPERSONALIZATION_QUERY"));
		}
	}

	// @Scope(value = "step")
	public class GdprBackupServiceProcessor implements ItemProcessor<BackupServiceInput, BackupServiceOutput> {
		private String CURRENT_CLASS = "GdprBackupServiceProcessor";
		private Map<String, String> categoryMap = null;
		private List<ImpactTable> impactTableDtls = null;
		Map<String, ImpactTable> mapImpacttable = null;
		Map<Integer, ImpactTable> mapWithIDKeyImpacttable = null;
		Map<String, List<String>> mapCountry = null;
		RunErrorMgmt runErrorMgmt = null;
		String strLastFetchDate = null;

		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "BackupbeforeStep";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method23w23. ");

			categoryMap = gdprInputDaoImpl.fetchCategoryDetails();
			impactTableDtls = gdprInputFetchDaoImpl.fetchImpactTable();
			mapImpacttable = impactTableDtls.stream()
					.collect(Collectors.toMap(ImpactTable::getImpactTableName, i -> i));
			mapWithIDKeyImpacttable = impactTableDtls.stream()
					.collect(Collectors.toMap(ImpactTable::getImpactTableId, i -> i));
			JobParameters jobParameters = stepExecution.getJobParameters();
			strLastFetchDate = gdprOutputDaoImpl.fetchLastDataLoad(GlobalConstants.TBL_GDPR_DEPERSONALIZATION__C);
			runId = jobParameters.getLong(GlobalConstants.JOB_REORGANIZE_INPUT_RUNID);
			System.out.println(
					CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: runId " + runId + "::::" + strLastFetchDate);
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: categoryMap " + categoryMap.toString());

		}

		@Override
		public BackupServiceOutput process(BackupServiceInput backupServiceInput) throws Exception {
			String CURRENT_METHOD = "Backupprocess";
			// System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside
			// method. ");

			if (categoryMap == null)
				categoryMap = gdprInputDaoImpl.fetchCategoryDetails();
			Boolean exceptionOccured = false;
			String backupDataStatus = "";

			String backpQuery = backupServiceInput.getBackupQuery();
			int catid = backupServiceInput.getCategoryId();
			String countrycode = backupServiceInput.getCountryCode();
			long sumId = backupServiceInput.getSummaryId();
			int impactTableId = backupServiceInput.getImpactTableId();

			long insertcount = 0;
			BackupServiceOutput backupServiceOutput = null;
			String backupTableName = "";
			//backupTableName = "BKP_" + impactTableName;
			String errorDetails = "";
			
			try {
				String impactTableName = mapWithIDKeyImpacttable.get(impactTableId).getImpactTableName();
				backupTableName = "BKP_" + impactTableName;
				String selectColumns = backpQuery.substring("SELECT ".length(), backpQuery.indexOf(" FROM "));

				List<String> stringList = Arrays.asList(selectColumns.split(","));
				List<String> trimmedStrings = new ArrayList<String>();
				for (String s : stringList) {
					trimmedStrings.add(s.trim());
				}
				Set<String> hSet = new HashSet<String>(trimmedStrings);

				selectColumns = hSet.stream().map(String::valueOf).collect(Collectors.joining(","));
				String splittedValues = hSet.stream().map(s -> s + "=excluded." + s).collect(Collectors.joining(","));

				String completeQuery = fetchCompleteBackupDataQuery(impactTableName, mapImpacttable, backupServiceInput,
						selectColumns, strLastFetchDate);
				completeQuery = completeQuery.replaceAll("TAG.", "SF_ARCHIVE.");
				@SuppressWarnings("unchecked")
				String backupDataInsertQuery = "INSERT INTO GDPR." + backupTableName + " (ID," + selectColumns + ") "
						+ completeQuery + " ON CONFLICT (id) DO UPDATE " + "  SET " + splittedValues + ";";
				insertcount = backupServiceDaoImpl.insertBackupTable(backupDataInsertQuery);

				// System.out.println("Inserted::"+insertcount+"backupDataInsertQuery::::::#$" +
				// backupDataInsertQuery);

				backupServiceOutput = new BackupServiceOutput(sumId, runId, insertcount);
			} catch (Exception exception) {
				exceptionOccured = true;
				backupDataStatus = GlobalConstants.ERR_DATABACKUP_PROCESS;
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getMessage();
				runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD,
						GlobalConstants.ERR_DATABACKUP_PROCESS, exception.getMessage());
			}
			try {
				if (exceptionOccured) {
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
							: GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
							GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
							moduleStartDateTime, backupDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
				if (runErrorMgmt != null) {
					gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
					throw new GdprException(GlobalConstants.ERR_DATABACKUP_PROCESS, errorDetails);
				}				
			} catch (GdprException exception) {
				backupDataStatus = backupDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
				errorDetails = errorDetails + exception.getMessage();
				throw new GdprException(backupDataStatus, errorDetails);
			} catch (Exception exception) {
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: "
						+ GlobalConstants.ERR_DATABACKUP_PROCESS + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
				exception.printStackTrace();
				errorDetails = errorDetails + exception.getMessage();
				throw new GdprException(
						GlobalConstants.ERR_DATABACKUP_PROCESS + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
			}
			return backupServiceOutput;
		}
	}

	public class BackupServiceOutputWriter<T> implements ItemWriter<BackupServiceOutput> {
		private String CURRENT_CLASS = "BackupServiceOutputWriter";

		private final BackupServiceDaoImpl bkpServiceDaoImpl;

		public BackupServiceOutputWriter(BackupServiceDaoImpl bkpServiceDaoImpl) {
			this.bkpServiceDaoImpl = bkpServiceDaoImpl;
		}

		@Override
		public void write(List<? extends BackupServiceOutput> lstBackupServiceOutput) throws Exception {
			String CURRENT_METHOD = "write";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
			Boolean exceptionOccured = false;
			String backupDataStatus = "";
			String errorDetails = "";
			
			try {
				bkpServiceDaoImpl.updateSummaryTable(lstBackupServiceOutput);
			} catch (Exception exception) {
				exceptionOccured = true;
				backupDataStatus = GlobalConstants.ERR_BACKUPSERVICE_DATA_COUNT;
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getMessage();
			}
			try {
				if (exceptionOccured) {
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
							: GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
							GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
							moduleStartDateTime, backupDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch (GdprException exception) {
				backupDataStatus = backupDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
				errorDetails = errorDetails + exception.getMessage();
				throw new GdprException(backupDataStatus, errorDetails);
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	public Step gdprBackupServiceStep() throws GdprException {
		String CURRENT_METHOD = "gdprBackupServiceStep";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
		Step step = null;
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		String errorDetails = "";
		
		try {
			step = stepBuilderFactory.get("gdprBackupServiceStep")
					.<BackupServiceInput, BackupServiceOutput>chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
					.reader(backupServiceReader(0)).processor(new GdprBackupServiceProcessor())
					.writer(new BackupServiceOutputWriter(backupServiceDaoImpl)).build();
		} catch (Exception exception) {
			exceptionOccured = true;
			backupDataStatus = GlobalConstants.ERR_BACKUPSERVICE_STEP;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if (exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
						: GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
						GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
						moduleStartDateTime, backupDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch (GdprException exception) {
			backupDataStatus = backupDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(backupDataStatus, errorDetails);
		}
		return step;
	}

	@Bean
	public Job processGdprBackupServiceJob() throws GdprException {
		String CURRENT_METHOD = "processGdprBackupServiceJob";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		Job job = null;
		String errorDetails = "";

		try {

			job = jobBuilderFactory.get("processGdprBackupServiceJob").incrementer(new RunIdIncrementer())
					.listener(backupListener(GlobalConstants.JOB_BACKUP_SERVICE_LISTENER)).flow(gdprBackupServiceStep())
					.end().build();
		} catch (Exception exception) {
			exceptionOccured = true;
			backupDataStatus = GlobalConstants.ERR_BACKUPSERVICE_JOB;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if (exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE
						: GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
						GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
						moduleStartDateTime, backupDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch (GdprException exception) {
			backupDataStatus = backupDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + backupDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(backupDataStatus, errorDetails);
		}
		return job;
	}

	@Bean
	public JobExecutionListener backupListener(String jobRelatedName) {
		String CURRENT_METHOD = "Backuplistener";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");

		return new BackupJobCompletionListener(jobRelatedName);
	}

	public String fetchCompleteBackupDataQuery(String tableName, Map<String, ImpactTable> mapImpactTable,
			BackupServiceInput backupServiceInput, String selectCls, String strLastFetchDate) {
		String CURRENT_METHOD = "fetchCompleteBackupDataQuery";
		// System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside
		// method");
		String taggedCompleteQuery = "";

		ImpactTable impactTable = mapImpactTable.get(tableName);
		String tableType = impactTable.getImpactTableType();
		// String parentTableName = impactTable.getParentTable();

		String[] aryParent = null;
		String[] aryParentSchema = null;
		String[] aryParentTableCol = null;

		if (impactTable.getParentTable().contains(":")) {
			aryParent = impactTable.getParentTable().split(":");
			aryParentSchema = impactTable.getParentSchema().split(":");
			aryParentTableCol = impactTable.getParentTableColumn().split(":");
		} else {
			aryParent = new String[1];
			aryParentSchema = new String[1];
			aryParentTableCol = new String[1];

			aryParent[0] = impactTable.getParentTable();
			aryParentSchema[0] = impactTable.getParentSchema();
			aryParentTableCol[0] = impactTable.getParentTableColumn();
		}
		selectCls = tableName + "." + selectCls.replaceAll(",", "," + tableName + ".");
		for (int aryParentIterator = 0; aryParentIterator < aryParent.length; aryParentIterator++) {
			String parentTableNm = aryParent[aryParentIterator];
			String parentSchema = aryParentSchema[aryParentIterator];
			String parentTableCol = aryParentTableCol[aryParentIterator];

			String taggedQuery = "";
			String query[] = new String[3];
			query[0] = "SELECT DISTINCT " + tableName + ".ID ID, " + selectCls;
			query[1] = " FROM " + impactTable.getImpactSchema() + "." + tableName + " " + tableName + ", "
					+ parentSchema + "." + parentTableNm + " " + parentTableNm;
			query[2] = " WHERE " + tableName + "." + impactTable.getImpactTableColumn() + "::varchar = " + parentTableNm
					+ "." + parentTableCol + "::varchar";
			ImpactTable currentImpactTable = mapImpactTable.get(parentTableNm);
			String currentTableType = currentImpactTable.getImpactTableType();
			String currentTableName = currentImpactTable.getImpactTableName();
			while (GlobalConstants.TYPE_TABLE_CHILD.equalsIgnoreCase(currentTableType)) {
				query[1] = query[1] + ", " + currentImpactTable.getParentSchema() + "."
						+ currentImpactTable.getParentTable() + " " + currentImpactTable.getParentTable();
				query[2] = query[2] + " AND " + currentTableName + "." + currentImpactTable.getImpactTableColumn()
						+ "::varchar = " + currentImpactTable.getParentTable() + "."
						+ currentImpactTable.getParentTableColumn() + "::varchar";
				currentImpactTable = mapImpactTable.get(currentImpactTable.getParentTable());
				currentTableType = currentImpactTable.getImpactTableType();
				currentTableName = currentImpactTable.getImpactTableName();
			}
			if (GlobalConstants.TYPE_TABLE_PARENT.equalsIgnoreCase(currentTableType)) {
				String colNames = currentImpactTable.getImpactColumns();
				String columnNames[];
				if (colNames.contains(GlobalConstants.COMMA_ONLY_STRING))
					columnNames = colNames.split(GlobalConstants.COMMA_ONLY_STRING);
				else {
					columnNames = new String[1];
					columnNames[0] = colNames;
				}

				query[2] = query[2] + " AND GDPR_DEPERSONALIZATION.CATEGORY_ID = " + backupServiceInput.getCategoryId()
						+ " AND GDPR_DEPERSONALIZATION.COUNTRY_CODE = '" + backupServiceInput.getCountryCode() + "'";

				if (strLastFetchDate != null) {
					query[2] = query[2] + " AND GDPR_DEPERSONALIZATION.CREATED_DATE_TIME >'" + strLastFetchDate + "'";
				}

			}
			taggedQuery = query[0] + query[1] + query[2];
			if (taggedCompleteQuery.equalsIgnoreCase(""))
				taggedCompleteQuery = taggedQuery;
			else
				taggedCompleteQuery = taggedCompleteQuery + " UNION " + taggedQuery;
		}

		return taggedCompleteQuery;

	}
}
