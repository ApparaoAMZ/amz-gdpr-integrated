package com.amazon.gdpr.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.input.Country;
import com.amazon.gdpr.model.gdpr.output.RunMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to fetch the RunMgmt table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class RunMgmtDaoImpl {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_RUNMGMTDAOIMPL;
	private static String RUNMGMT_LAST_RUN 			= SqlQueriesConstant.RUNMGMT_LAST_RUN;
	private static String RUNMGMT_INSERT_RUN		= SqlQueriesConstant.RUNMGMT_INSERT_RUN;
	private static String RUN_MODULE_MGMT_INSERT	= SqlQueriesConstant.RUN_MODULE_MGMT_INSERT;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	 
	/**
	 * Fetches the last run detail from the RunMgmt table
	 * @return RunMgmt 
	 */
	public RunMgmt fetchLastRunDetail(){
		String CURRENT_METHOD = "fetchLastRunDetail";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		RunMgmt runMgmt = null;		
		@SuppressWarnings("unchecked")
		List<RunMgmt> lstLastRunMgmt = jdbcTemplate.query(RUNMGMT_LAST_RUN, new RunMgmtRowMapper());		
		if(lstLastRunMgmt != null && lstLastRunMgmt.size() > 0){
			runMgmt = lstLastRunMgmt.get(0);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Fetch Last Run Detail : "+runMgmt.getRunId());
		}		
		return runMgmt;
	}
	
	/**
	 * Inserts a new row in RUN_MGMT
	 * @param runName
	 */
	public void initiateNewRun(String runName) {
		String CURRENT_METHOD = "initiateNewRun";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		jdbcTemplate.update(RUNMGMT_INSERT_RUN, new Object[]{runName, GlobalConstants.STATUS_INPROGRESS});			
	}	
	
	/****************************************************************************************
	 * This rowMapper converts the row data from RunMgmt table to RunMgmt Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class RunMgmtRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_RUNMGMTROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public RunMgmt mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			RunMgmt runMgmt = new RunMgmt(rs.getInt("RUN_ID"), rs.getString("RUN_STATUS"));
			return runMgmt;
		}
	}
		
	/**
	 * Inserts a row in RUN_MODULE_MGMT
	 * This method is called at the end of each service
	 * @param runModuleMgmt
	 */
	public void insertModuleUpdates(RunModuleMgmt runModuleMgmt) {
		String CURRENT_METHOD = "insertModuleUpdates";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
				
		jdbcTemplate.update(RUN_MODULE_MGMT_INSERT, new Object[]{runModuleMgmt.getRunId(), runModuleMgmt.getModuleName(), 
				runModuleMgmt.getSubModuleName(), runModuleMgmt.getModuleStatus(), runModuleMgmt.getModuleStartDateTime(), 
				runModuleMgmt.getModuleEndDateTime(), runModuleMgmt.getComments(), runModuleMgmt.getErrorDetails()});			
	}
	
	
	public List<RunModuleMgmt> prevModuleRunStatus(long runId) {
		String CURRENT_METHOD = "prevModuleRunStatus";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<RunModuleMgmt> lstRunModuleMgmt = jdbcTemplate.query(SqlQueriesConstant.RUN_MODULE_STATUS_FETCH, new RunModuleMgmtRowMapper());
		return lstRunModuleMgmt;
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from RUN_MODULE_MGMT table to RunModuleMgmt Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class RunModuleMgmtRowMapper implements RowMapper{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_RUNMODULEMGMTROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public RunModuleMgmt mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new RunModuleMgmt(rs.getString("MODULE_STATUS"));
		}
	}

}