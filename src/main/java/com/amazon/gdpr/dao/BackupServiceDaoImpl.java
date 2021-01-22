package com.amazon.gdpr.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.BackupServiceOutput;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and
 * Connects with the schema to load the Hvh Output table rows
 ****************************************************************************************/
@Transactional
@Repository
public class BackupServiceDaoImpl {

	private static String CURRENT_CLASS = "BackupServiceDaoImpl";

	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	/**
	 * This method inserts the backup data
	 * 
	 * @param backupdatainsertquery
	 * @return data insterted count
	 */

	@Transactional
	public int insertBackupTable(String backupDataInsertQuery) {
		String CURRENT_METHOD = "insertBackupTable";
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		// int ainsertedCount=jdbcTemplate.update(backupDataInsertQuery);
		int ainsertedCount = jdbcTemplate.update(backupDataInsertQuery);
		return ainsertedCount;
	}

	
	@Transactional
	public void updateSummaryTable(List<? extends BackupServiceOutput> lstBackupServiceOutput) {
		
				
	    //String sql = "UPDATE GDPR.RUN_SUMMARY_MGMT SET BACKUP_ROW_COUNT=? WHERE SUMMARY_ID=? AND RUN_ID=?";
	    String CURRENT_METHOD = "batchInsertGdprDepersonalizationOutput";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(SqlQueriesConstant.UPDATE_BACKUPROW_COUNT, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				BackupServiceOutput backupServiceOutput = (BackupServiceOutput) lstBackupServiceOutput.get(i);
				ps.setLong(1, backupServiceOutput.getBackupRowCount());
				ps.setLong(2, backupServiceOutput.getSummaryId());
				ps.setLong(3, backupServiceOutput.getRunId());
				
				
			}
			public int getBatchSize() {
				return lstBackupServiceOutput.size();
			}
		});

	}
}