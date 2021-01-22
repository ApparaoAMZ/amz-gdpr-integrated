package com.amazon.gdpr.configuration;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazon.gdpr.batch.ReorganizeInputCompletionListener;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.HvhOutputDaoImpl;
import com.amazon.gdpr.model.GdprDepersonalizationInput;
import com.amazon.gdpr.model.GdprDepersonalizationOutput;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the Reading of SALESFORCE.GDPR_DEPERSONALIZATION__C table 
 * and Writing into GDPR.GDPR_DEPERSONALIZATION
 ****************************************************************************************/
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@Configuration
public class ReOrganizeInputBatchConfig {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_REORGANIZEINPUT_BATCHCONFIG;
			
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;

	@Autowired
	public HvhOutputDaoImpl hvhOutputDaoImpl; 
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	public long runId;
	public Date moduleStartDateTime = null; 
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<GdprDepersonalizationInput> gdprDepersonalizationDBreader(@Value("#{jobParameters[RunId]}") long runId) throws GdprException {
		String CURRENT_METHOD = "reader";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		JdbcCursorItemReader<GdprDepersonalizationInput> reader = null;
		Boolean exceptionOccured = false;
		String reOrganizeDataStatus = "";
		String errorDetails = "";
		
		try {
			String gdprDepersonalizationDataFetch = SqlQueriesConstant.GDPR_DEPERSONALIZATION_FETCH ;
			String strLastFetchDate = gdprOutputDaoImpl.fetchLastDataLoad(GlobalConstants.TBL_GDPR_DEPERSONALIZATION__C);
			
			if(strLastFetchDate != null) {
				//gdprDepersonalizationDataFetch = gdprDepersonalizationDataFetch + " AND (CREATEDDATE > TO_DATE(\'"+strLastFetchDate+
								//"\', \'"+ GlobalConstants.DATE_FORMAT +"\') OR LASTMODIFIEDDATE > TO_DATE(\'"+strLastFetchDate+
								//"\', \'"+ GlobalConstants.DATE_FORMAT +"\' )) ";
				gdprDepersonalizationDataFetch = gdprDepersonalizationDataFetch + " AND (CREATEDDATE > '"+strLastFetchDate+"' OR LASTMODIFIEDDATE > '"+strLastFetchDate+"') ";
				
			} 
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: GDPR Depersonalization Data Fetch Query : "+gdprDepersonalizationDataFetch); 
			reader = new JdbcCursorItemReader<GdprDepersonalizationInput>();
			reader.setDataSource(dataSource);
			reader.setSql(gdprDepersonalizationDataFetch);
			reader.setRowMapper(new GdprDepersonalizationInputRowMapper());
		} catch (Exception exception) {
			exceptionOccured = true;
			reOrganizeDataStatus  = "Facing issues in reading GDPR_Depersonalization table. " ;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			throw new GdprException(reOrganizeDataStatus, errorDetails); 
		}		
		return reader;
	}
	
	//To set values into GdprDepersonalizationInput Object
	public class GdprDepersonalizationInputRowMapper implements RowMapper<GdprDepersonalizationInput> {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRDEPERSONALIZATIONINPUTROWMAPPER;
		
		@Override
		public GdprDepersonalizationInput mapRow(ResultSet rs, int rowNum) throws SQLException {			
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			
			String candidateId = rs.getString("CANDIDATE__C"); 
			String candidateOrApplicationId = (candidateId != null && ! (GlobalConstants.EMPTY_STRING.equalsIgnoreCase(candidateId.trim()))) ? 
					candidateId.trim() : rs.getString("BGC_Application__c");
			return new GdprDepersonalizationInput(
					candidateOrApplicationId, rs.getString("CATEGORY__C"), rs.getString("COUNTRY_CODE__C"), rs.getString("BGC_STATUS__C"),
					rs.getString("PH_AMAZON_ASSESSMENT_STATUS__C"), rs.getString("PH_CANDIDATE_PROVIDED_STATUS__C"), 
					rs.getString("PH_MASTER_DATA_STATUS__C"), rs.getString("PH_WITH_CONSENT_STATUS__C"));
		}
	}
	
	//@Scope(value = "step")
	public class ReorganizeDataProcessor implements ItemProcessor<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>>{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_REORGANIZEDATAPROCESSOR;
		private Map<String, String> mapCategory = null;
		private Map<String, String> mapFieldCategory = null;
		//private Map<String, String> fieldCategoryMap = null;
		
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "beforeStep";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
			
			mapCategory = gdprInputDaoImpl.fetchCategoryDetails();
			mapFieldCategory = gdprInputDaoImpl.getMapFieldCategory();
			
			JobParameters jobParameters = stepExecution.getJobParameters();
			runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
			long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: currentRun "+currentRun);
		    System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: mapCategory "+mapCategory.toString());
		    System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: fieldCategoryMap "+mapFieldCategory.toString());
		}
				
		@Override
		public List<GdprDepersonalizationOutput> process(GdprDepersonalizationInput gdprDepersonalizationInput) throws GdprException {
			String CURRENT_METHOD = "process";		
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
			Boolean exceptionOccured = false;
			String reOrganizeDataStatus = "";
			List<GdprDepersonalizationOutput> lstGdprDepersonalizationOutput = new ArrayList<GdprDepersonalizationOutput>();
			String errorDetails = "";
			
			try{
				if(mapCategory == null)	
					mapCategory = gdprInputDaoImpl.fetchCategoryDetails();
				if(mapFieldCategory == null)
					mapFieldCategory = gdprInputDaoImpl.getMapFieldCategory();
				List<String> fieldCategoryList = new ArrayList<String>(mapFieldCategory.keySet());				
				for(String fieldCategory : fieldCategoryList){					
					Field field = GdprDepersonalizationInput.class.getDeclaredField(fieldCategory);
					String fieldValue = (String) field.get(gdprDepersonalizationInput);
					//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: fieldCategory : fieldValue "+fieldCategory+" : "+fieldValue);
					if(GlobalConstants.STATUS_CLEARED.equalsIgnoreCase(fieldValue)){
						GdprDepersonalizationOutput gdprDepersonalizationOutput = new GdprDepersonalizationOutput(runId,
							gdprDepersonalizationInput.getCandidate(), Integer.parseInt(mapFieldCategory.get(fieldCategory)), 
							gdprDepersonalizationInput.getCountryCode(), GlobalConstants.STATUS_CLEARED, 
							GlobalConstants.STATUS_SCHEDULED);
						lstGdprDepersonalizationOutput.add(gdprDepersonalizationOutput);
					}
				}
			} catch (Exception exception) {
				exceptionOccured = true;
				reOrganizeDataStatus  = "Facing issues while processing data. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getMessage();
			}
			try {
				if(exceptionOccured){
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
							GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
							moduleStartDateTime, reOrganizeDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch(GdprException exception) {
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
				errorDetails = errorDetails + exception.getMessage();
				throw new GdprException(reOrganizeDataStatus, errorDetails); 
			}
			try{
				hvhOutputDaoImpl.batchInsertGdprDepersonalizationOutput(lstGdprDepersonalizationOutput);
			} catch (Exception exception) {
				exceptionOccured = true;
				reOrganizeDataStatus  = "Facing issues while writing data into GDPR_Depersonalization table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
				exception.printStackTrace();
				errorDetails = errorDetails + exception.getMessage(); 
			}
			try {
				if(exceptionOccured){
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
							GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
							moduleStartDateTime, reOrganizeDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch(GdprException exception) {
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
				errorDetails = errorDetails + exception.getMessage();
				throw new GdprException(reOrganizeDataStatus, errorDetails); 
			}
			return lstGdprDepersonalizationOutput;
		}
	}
	
	/*public class ReorganizeOutputWriter<T> implements ItemWriter<GdprDepersonalizationOutput> { 
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_REORGANIZEINPUTWRITER;
		long runId = 0;
									
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "beforeStep";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
						
			JobParameters jobParameters = stepExecution.getJobParameters();
			runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
			long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: currentRun "+currentRun);		    
		}
		
		@Override
		public void write(List<? extends GdprDepersonalizationOutput> lstGdprDepersonalizationOutput) throws GdprException {
			String CURRENT_METHOD = "write";
			Boolean exceptionOccured = false;
			String reOrganizeDataStatus = "";
			
			try{
				hvhOutputDaoImpl.batchInsertGdprDepersonalizationOutput(lstGdprDepersonalizationOutput);
			} catch (Exception exception) {
				exceptionOccured = true;
				reOrganizeDataStatus  = "Facing issues while writing data into GDPR_Depersonalization table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
				exception.printStackTrace();
			}
			try {
				if(exceptionOccured){
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
							GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
							moduleStartDateTime, reOrganizeDataStatus);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch(GdprException exception) {
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
				throw new GdprException(reOrganizeDataStatus); 
			}
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		}		
	}*/
			
	@Bean
	public Step reorganizeInputStep() throws GdprException {
		String CURRENT_METHOD = "reorganizeInputStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		Boolean exceptionOccured = false;
		String reOrganizeDataStatus = "";
		String errorDetails = "";

		try {
			step = stepBuilderFactory.get("reorganizeInputStep")
				.<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
				.reader(gdprDepersonalizationDBreader(0))
				.processor(new ReorganizeDataProcessor())
				//.writer(new ReorganizeOutputWriter())
				.build();
		} catch (Exception exception) {
			exceptionOccured = true;
			reOrganizeDataStatus  = GlobalConstants.ERR_GDPR_DEPERSONALIZATION_LOAD ;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(reOrganizeDataStatus, errorDetails); 
		}
		return step;		
	}
	
	@Bean
	public Job processreorganizeInputJob() throws GdprException {
		String CURRENT_METHOD = "processreorganizeInputJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		Job job = null;
		Boolean exceptionOccured = false;
		String reOrganizeDataStatus = "";
		String errorDetails = "";
		
		try{
			job = jobBuilderFactory.get(CURRENT_METHOD)
					.incrementer(new RunIdIncrementer()).listener(reorganizeInputlistener(GlobalConstants.JOB_REORGANIZE_INPUT_PROCESSOR))										
					.flow(reorganizeInputStep())
					.end()
					.build();
		} catch(Exception exception) {
			exceptionOccured = true;
			reOrganizeDataStatus = GlobalConstants.ERR_REORGANIZE_JOB_PROCESS;
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(reOrganizeDataStatus, errorDetails); 
		}
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public JobExecutionListener reorganizeInputlistener(String jobRelatedName) {
		String CURRENT_METHOD = "reorganizeInputlistener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new ReorganizeInputCompletionListener(jobRelatedName);
	}	
}