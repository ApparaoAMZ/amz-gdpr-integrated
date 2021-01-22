package com.amazon.gdpr.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.output.DataLoad;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.model.gdpr.output.SummaryData;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to fetch the Output table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class GdprOutputDaoImpl {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPROUTPUTDAOIMPL;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * This method inserts the RunAnonymizationMapping values 
	 * @param lstImpactField
	 * @return
	 */	
	public int batchInsertRunAnonymizeMapping(long runId, String countryCode, String region) {
		String CURRENT_METHOD = "fetchCategoryDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		return jdbcTemplate.update(SqlQueriesConstant.RUN_ANONYMIZATION_INSERT, new Object[]{runId, countryCode, region, countryCode});		
	}
	
	/**
	 * This method is called whenever there is an error 
	 * The error details are loaded in this method
	 * @param runErrorMgmt 
	 */
	@Transactional
	public void loadErrorDetails(RunErrorMgmt runErrorMgmt) {
		String CURRENT_METHOD = "loadErrorDetails";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		jdbcTemplate.update(SqlQueriesConstant.RUN_ERROR_MGMT_INSERT, new Object[]{runErrorMgmt.getRunId(), runErrorMgmt.getErrorSummary(), 
				runErrorMgmt.getErrorDetail()});
	}
	
	public void loadDataProcess(long runId) {
		
	}
		
	/**
	 * Fetches the Summary Details rows
	 * @return List<SummaryData>
	 */
	public List<SummaryData> fetchSummaryDetails(long runId){
		String CURRENT_METHOD = "fetchSummaryDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<SummaryData> lstSummaryData = jdbcTemplate.query(SqlQueriesConstant.SUMMARY_DATA_FETCH, new Object[]{runId}, new SummaryDataRowMapper());	
		return lstSummaryData;		
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from IMPACTTABLE table to ImpactTable Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class SummaryDataRowMapper implements RowMapper {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS = GlobalConstants.CLS_SUMMARYDATAROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public SummaryData mapRow(ResultSet rs, int rowNum) throws SQLException {
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new SummaryData(rs.getInt("CATEGORY_ID"), rs.getString("REGION"), rs.getString("COUNTRY_CODE"),   
					rs.getString("IMPACT_TABLE_NAME"), rs.getString("IMPACT_SCHEMA"), rs.getString("IMPACT_FIELD_NAME"), 
					rs.getString("IMPACT_FIELD_TYPE"), rs.getString("TRANSFORMATION_TYPE"), rs.getInt("IMPACT_TABLE_ID"));
		}
	}
	
	/**
	 * This method inserts the RunSummaryMgmt values 
	 * @param lstRunSummaryMgmt
	 * @return
	 */
	@Transactional
	public void insertRunSummaryMgmt(List<RunSummaryMgmt> lstRunSummaryMgmt) {
		String CURRENT_METHOD = "insertRunSummaryMgmt";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		jdbcTemplate.batchUpdate(SqlQueriesConstant.RUN_SUMMARY_MGMT_INSERT, new BatchPreparedStatementSetter() {

	        @Override
	        public void setValues(PreparedStatement ps, int i) throws SQLException {
	        	RunSummaryMgmt runSummaryMgmt = lstRunSummaryMgmt.get(i);
	        	ps.setObject(1, runSummaryMgmt.getRunId());
				ps.setInt(2, runSummaryMgmt.getCategoryId());
				ps.setString(3, runSummaryMgmt.getRegion());
				ps.setString(4, runSummaryMgmt.getCountryCode());
				ps.setInt(5, runSummaryMgmt.getImpactTableId());
				ps.setString(6, runSummaryMgmt.getBackupQuery());
				ps.setString(7, runSummaryMgmt.getDepersonalizationQuery());
	        }
	        @Override
	        public int getBatchSize() {
	            return lstRunSummaryMgmt.size();
	        }
	    });		
	}	
	
	/**
	 * This method inserts the RunSummaryMgmt values 
	 * @param lstRunSummaryMgmt
	 * @param batchSize
	 */
	@Transactional
	public void batchInsertRunSummaryMgmt(List<RunSummaryMgmt> lstRunSummaryMgmt, int batchSize) {
		String CURRENT_METHOD = "batchInsertRunSummaryMgmt";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[][] insertCounts = jdbcTemplate.batchUpdate(SqlQueriesConstant.RUN_SUMMARY_MGMT_INSERT, lstRunSummaryMgmt, batchSize, 
						new ParameterizedPreparedStatementSetter<RunSummaryMgmt>() {
			public void setValues(PreparedStatement ps, RunSummaryMgmt runSummaryMgmt) throws SQLException {				
				ps.setObject(1, runSummaryMgmt.getRunId());
				ps.setInt(2, runSummaryMgmt.getCategoryId());
				ps.setString(3, runSummaryMgmt.getRegion());
				ps.setString(4, runSummaryMgmt.getCountryCode());
				ps.setInt(5, runSummaryMgmt.getImpactTableId());
				ps.setString(6, runSummaryMgmt.getImpactTableName());
				ps.setString(7, runSummaryMgmt.getBackupQuery());
				ps.setString(8, runSummaryMgmt.getDepersonalizationQuery());	
				ps.setString(9, runSummaryMgmt.getTaggedQueryLoad());
			}
		});
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: insertCounts"+insertCounts);
	}
	
	/**
	 * This method inserts the DataLoad values 
	 * @param dataLoad the column values required for the table
	 * @return
	 */	
	public int insertDataLod(DataLoad dataLoad) {
		String CURRENT_METHOD = "insertDataLod";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		return jdbcTemplate.update(SqlQueriesConstant.DATA_LOAD_INSERT, new Object[]{dataLoad.getTableName(), dataLoad.getCountryCode(),
				dataLoad.getDataLoadDateTime()});
	}
	
	/**
	 * Fetches the Last Data Load detail of a particular table
	 * @return Date The date when the last data was loaded in the table
	 */
	public String fetchLastDataLoad(String tableName) {
		String CURRENT_METHOD = "fetchLastDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		String strDataLoadDate = null;
		@SuppressWarnings("unchecked")
		List<DataLoad> lstDataLoad = jdbcTemplate.query(SqlQueriesConstant.LAST_DATA_LOAD_FETCH, new Object[]{tableName}, new DataLoadRowMapper());
		if(lstDataLoad != null && lstDataLoad.size() > 0)
			strDataLoadDate = lstDataLoad.get(0).getStrDataLoadDate();
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: strDataLoadDate : "+strDataLoadDate);
		return strDataLoadDate;
	}
		
	/****************************************************************************************
	 * This rowMapper converts the row data from DATA_LOAD table to DataLoad Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class DataLoadRowMapper implements RowMapper {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS = GlobalConstants.CLS_DATALOADROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public DataLoad mapRow(ResultSet rs, int rowNum) throws SQLException {
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			//return new DataLoad(rs.getDate("LAST_DATA_LOADED_DATE"));
			return new DataLoad(rs.getString("STR_DATA_LOADED_DATE"));
		}
	}
	
	
	/**
	 * Fetches the Run Summary Details rows
	 * @return List<RunSummaryMgmt>
	 */
	public List<RunSummaryMgmt> fetchRunSummaryDetail(long runId) {
		String CURRENT_METHOD = "fetchRunSummaryDetail";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		
		@SuppressWarnings("unchecked")
		List<RunSummaryMgmt> lstRunSummaryMgmt = jdbcTemplate.query(SqlQueriesConstant.RUN_SUMMARY_MGMT_LIST_FETCH, 
				new Object[]{runId}, new RunSummaryMgmtRowMapper());		
		return lstRunSummaryMgmt;
	}

	/**
	 * Fetches the Run Summary Details rows
	 * @return List<RunSummaryMgmt>
	 */
	public RunSummaryMgmt fetchRunSummaryDetail(long runId, long runSummaryId) {
		String CURRENT_METHOD = "fetchRunSummaryDetail";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		
		@SuppressWarnings("unchecked")
		List<RunSummaryMgmt> lstRunSummaryMgmt = jdbcTemplate.query(SqlQueriesConstant.RUN_SUMMARY_MGMT_ROW_FETCH, 
				new Object[]{runId, runSummaryId}, new RunSummaryMgmtRowMapper());
		RunSummaryMgmt runSummaryMgmt = (lstRunSummaryMgmt != null && lstRunSummaryMgmt.size() > 0)? lstRunSummaryMgmt.get(0) : null;
		return runSummaryMgmt;
	}

	
	/****************************************************************************************
	 * This rowMapper converts the row data from RUN_SUMMARY_MGMT table to RunSummaryMgmt Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class RunSummaryMgmtRowMapper implements RowMapper {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS = GlobalConstants.CLS_RUNSUMMARYMGMT_ROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public RunSummaryMgmt mapRow(ResultSet rs, int rowNum) throws SQLException {
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				
			return new RunSummaryMgmt(rs.getLong("SUMMARY_ID"), rs.getLong("RUN_ID"), rs.getInt("CATEGORY_ID"), 
					rs.getString("COUNTRY_CODE"), rs.getInt("IMPACT_TABLE_ID"), rs.getString("IMPACT_TABLE_NAME"), 
					rs.getString("BACKUP_QUERY"), rs.getString("DEPERSONALIZATION_QUERY"), rs.getString("TAGGED_QUERY"));	
		}
	}
	
	/**
	 * Fetches the Last Data Load detail of a particular table
	 * @return Date The date when the last data was loaded in the table
	 */
	public List<RunModuleMgmt> fetchLastModuleData(long runId) {
		String CURRENT_METHOD = "fetchLastModuleData";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		@SuppressWarnings("unchecked")
		List<RunModuleMgmt> lstRunModuleMgmt= jdbcTemplate.query(SqlQueriesConstant.LAST_MODULE_DATA_FETCH, new Object[]{runId}, new ModuleDataRowMapper());
		return lstRunModuleMgmt;
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from DATA_LOAD table to DataLoad Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ModuleDataRowMapper implements RowMapper {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS = GlobalConstants.CLS_DATALOADROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public RunModuleMgmt mapRow(ResultSet rs, int rowNum) throws SQLException {
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			//return new DataLoad(rs.getDate("LAST_DATA_LOADED_DATE"));
			return new RunModuleMgmt(rs.getLong("RUN_ID"),rs.getString("MODULE_NAME"),rs.getString("SUBMODULE_NAME"),rs.getString("MODULE_STATUS"),rs.getDate("MODULE_START_DATE_TIME"),rs.getDate("MODULE_END_DATE_TIME"),rs.getString("MODULE_COMMENTS"));
		}
	}

}