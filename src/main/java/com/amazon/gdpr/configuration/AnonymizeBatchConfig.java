package com.amazon.gdpr.configuration;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;

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
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.batch.AnonymizeJobCompletionListener;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.archive.AnonymizeTable;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the anonymization of SF_ARCHIVE.<Tables>
 ****************************************************************************************/
@SuppressWarnings("unused")
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@Configuration
public class AnonymizeBatchConfig {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZE_BATCH_CONFIG;
	
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
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	public long runId;
	public long runSummaryId;	
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<AnonymizeTable> anonymizeTableReader(@Value("#{jobParameters[RunId]}") long runId,
			@Value("#{jobParameters[RunSummaryId]}") long runSummaryId) throws GdprException {
		String CURRENT_METHOD = "reader";
		//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		JdbcCursorItemReader<AnonymizeTable> reader = null;
		String anonymizeDataStatus = "";
		String errorDetails = "";
		Boolean exceptionOccured = false;
		AnonymizeTable anonymizeTable = null;
		RunSummaryMgmt runSummaryMgmt = null;
		
		try {
			runSummaryMgmt = gdprOutputDaoImpl.fetchRunSummaryDetail(runId, runSummaryId);
		} catch (Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus  = "Facing issues in reading runSummaryDetail for run - "+runId+" for run summary id -"
						+runSummaryId;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getStackTrace().toString();
		}			
		try {
			if (runSummaryMgmt != null) {
				String taggedQueryFetch = "SELECT ID FROM "+"TAG."+runSummaryMgmt.getImpactTableName()+" WHERE CATEGORY_ID = "+
						runSummaryMgmt.getCategoryId()+" AND COUNTRY_CODE = \'"+runSummaryMgmt.getCountryCode()+"\' AND STATUS = \'SCHEDULED\'";
				anonymizeTable = new AnonymizeTable(runId, runSummaryMgmt.getImpactTableId(), runSummaryMgmt.getImpactTableName(),
						runSummaryMgmt.getCountryCode(), runSummaryMgmt.getCategoryId(), runSummaryMgmt.getDepersonalizationQuery(), 0, null);
				reader = new JdbcCursorItemReader<AnonymizeTable>();
				reader.setDataSource(dataSource);
				reader.setSql(taggedQueryFetch);
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: taggedQueryFetch "+taggedQueryFetch);
				reader.setRowMapper(new AnonymizeTableRowMapper(anonymizeTable));
			}
			
			/*if(lstRunSummaryMgmt != null) {
				for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) { 
					// TODO Add tagged data query in RunSummaryMgmt 
					String taggedQueryFetch = "SELECT ID FROM "+"TAG."+runSummaryMgmt.getImpactTableName()+" WHERE CATEGORY_ID = "+
							runSummaryMgmt.getCategoryId()+" AND COUNTRY_CODE = \'"+runSummaryMgmt.getCountryCode()+"\' AND STATUS = \'SCHEDULED\'";
					anonymizeTable = new AnonymizeTable(runId, runSummaryMgmt.getImpactTableId(), runSummaryMgmt.getImpactTableName(),
							runSummaryMgmt.getCountryCode(), runSummaryMgmt.getCategoryId(), runSummaryMgmt.getDepersonalizationQuery(), 0, null);
					reader = new JdbcCursorItemReader<AnonymizeTable>();
					reader.setDataSource(dataSource);
					reader.setSql(taggedQueryFetch);
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: sqlQuery "+taggedQueryFetch);
					reader.setRowMapper(new AnonymizeTableRowMapper(anonymizeTable));
				}
			}*/
		} catch (Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus  = "Facing issues in reading tagged tables. " ;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
			errorDetails = exception.getStackTrace().toString(); 
			exception.printStackTrace();			
		}
		try {
			if(exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, anonymizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails + exception.getStackTrace().toString();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}
		return reader;
	}
	
	//To set values into Tag Tables Object
	public class AnonymizeTableRowMapper implements RowMapper<AnonymizeTable> {
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZETABLEROWMAPPER;
		String tableName;
		AnonymizeTable updatedAnonymizeTable;
		
		public AnonymizeTableRowMapper(AnonymizeTable anonymizeTable) {			
			this.updatedAnonymizeTable = anonymizeTable;
		}
		
		@Override
		public AnonymizeTable mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//List<String> lstAnonymizeId = new ArrayList<String>();
			this.updatedAnonymizeTable.setAnonymizeId(rs.getInt("ID"));
	        return this.updatedAnonymizeTable;
		}
	}
	
	public class AnonymizeProcessor implements ItemProcessor<AnonymizeTable, AnonymizeTable> {
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_ANONYMIZEPROCESSOR;
		//private Map<String, String> mapTableIdToName = null;
		
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "beforeStep";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
			
			long currentRun;
			
			JobParameters jobParameters = stepExecution.getJobParameters();
			runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
			currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
			runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
			
			//mapTableIdToName = gdprInputDaoImpl.fetchImpactTableMap(GlobalConstants.IMPACTTABLE_MAP_IDTONAME);			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);			
		}
		
		@Override
		public AnonymizeTable process(AnonymizeTable arg0) throws Exception {
			String CURRENT_METHOD = "process";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");		
			//arg0.setTableName(mapTableIdToName.get(arg0.getTableId()));
			return arg0;
		}
	}
	
		
	public class AnonymizeInputWriter<T> implements ItemWriter<AnonymizeTable> { 
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZEINPUTWRITER;		
		private final JdbcTemplate jdbcTemplate;	
		
		public AnonymizeInputWriter(JdbcTemplate jdbcTemplate) {
			this.jdbcTemplate = jdbcTemplate;
		}
				
		@Transactional
		@Override
		public void write(List<? extends AnonymizeTable> lstAnonymizeTable) throws GdprException {
			String CURRENT_METHOD = "write";
			Boolean exceptionOccured = false;
			String anonymizeDataStatus = "";
			String errorDetails = "";
			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			try {
				if(lstAnonymizeTable != null && lstAnonymizeTable.size() > 0){
					
					JdbcBatchItemWriter<AnonymizeTable> databaseItemWriter  = new JdbcBatchItemWriter<AnonymizeTable>();
					databaseItemWriter.setDataSource(dataSource);
					//itemWritter.(jdbcTemplate);
					String depersonalizeQuery = lstAnonymizeTable.get(0).getDepersonalizationQuery();
					databaseItemWriter.setSql(depersonalizeQuery);
					databaseItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<AnonymizeTable>());
					//return itemWritter;
					
					/*AnonymizeTable firstRowAnonymizeTable = lstAnonymizeTable.get(0);					
					String depersonalizeQuery = firstRowAnonymizeTable.getDepersonalizationQuery();
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: depersonalizeQuery "+depersonalizeQuery);
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizeTable size "+lstAnonymizeTable.size());
					
					/*List<Object[]> lstId = new ArrayList<Object[]>();
					lstAnonymizeTable.forEach(anonymizeTable -> lstId.add(new Object[] {anonymizeTable.getAnonymizeId()}));
					//int[] depersonalizeCount  = jdbcTemplate.batchUpdate(depersonalizeQuery,  lstId);

					int[] depersonalizeCount  = jdbcTemplate.batchUpdate(depersonalizeQuery, new BatchPreparedStatementSetter() {
						
		                public void setValues(PreparedStatement ps, int i) throws SQLException {
		                    ps.setLong(1, lstAnonymizeTable.get(i).getAnonymizeId());                    
		                }
		
		                public int getBatchSize() {
		                    return lstAnonymizeTable.size();
		                }		        
			        });*/
					//TODO Update Depersonalization Count in RunSummaryMgmt table
				}				
			} catch (Exception exception) {
				exceptionOccured = true;
				anonymizeDataStatus  = "Facing issues while anonymizing data in archival table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			}
			try {
				if(exceptionOccured){
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
							GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, moduleStatus, moduleStartDateTime, 
							moduleStartDateTime, anonymizeDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch(GdprException exception) {
				anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
				errorDetails = errorDetails + exception.getStackTrace().toString();
				throw new GdprException(anonymizeDataStatus, errorDetails); 
			}
		}
	}
	
	@Bean
	public Step anonymizeStep() throws GdprException {
		String CURRENT_METHOD = "anonymizeStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		Boolean exceptionOccured = false;				
		String anonymizeStatus = "";
		String errorDetails = "";
	
		try {			
			step = stepBuilderFactory.get(CURRENT_METHOD)
				.<AnonymizeTable, AnonymizeTable> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
				.reader(anonymizeTableReader(0,0))
				.processor(new AnonymizeProcessor())
				.writer(new AnonymizeInputWriter<Object>(jdbcTemplate))
				.build();
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_TAGGING_LOAD);
			exceptionOccured = true;
			exception.printStackTrace();
			anonymizeStatus  = GlobalConstants.ERR_TAGGING_LOAD ;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+anonymizeStatus);
			errorDetails = exception.getStackTrace().toString();
		}
		try {
			if(exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, anonymizeStatus, errorDetails);				
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			anonymizeStatus = anonymizeStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeStatus);
			errorDetails = errorDetails + exception.getStackTrace().toString();
			throw new GdprException(anonymizeStatus, errorDetails); 
		}
		return step;
	}
		
	@Bean
	public Job processAnonymizeJob() throws GdprException {
		String CURRENT_METHOD = "processAnonymizeJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Job job = null;
		Boolean exceptionOccured = false;
		String anonymizeDataStatus = "";
		String errorDetails = "";
		
		try {
			job = jobBuilderFactory.get(CURRENT_METHOD)
					.incrementer(new RunIdIncrementer()).listener(anonymizeListener(GlobalConstants.JOB_ANONYMIZE))										
					.flow(anonymizeStep())
					.end()
					.build();
		} catch(Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus = GlobalConstants.ERR_ANONYMIZE_JOB_RPOCESS;
			exception.printStackTrace();
			errorDetails = exception.getStackTrace().toString();
		}
		try {
			if(exceptionOccured) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, anonymizeDataStatus, errorDetails);	
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails + exception.getStackTrace().toString();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public JobExecutionListener anonymizeListener(String jobRelatedName) {
		String CURRENT_METHOD = "listener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new AnonymizeJobCompletionListener(jobRelatedName);
	}
}