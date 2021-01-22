package com.amazon.gdpr.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and
 * Connects with the schema to fetch the Impact table and parent table rows
 * Connects with the schema to fetch the backup table rows
 ****************************************************************************************/
@Transactional
@Repository
public class BackupTableProcessorDaoImpl {

	private static String CURRENT_CLASS = GlobalConstants.CLS_BACKUPTABLEPROCESSORDAOIMPL;

	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	/**
	 * Fetches the BackupTableDetails Table rows
	 * 
	 * @return List of BackupTable Details
	 */
	public List<BackupTableDetails> fetchBackupTableDetails() {
		String CURRENT_METHOD = "fetchBackupTableColumn";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		List<BackupTableDetails> lstBackupTableDetails = jdbcTemplate.query(SqlQueriesConstant.BACKUPTABLE_COLUMNS_QRY,
				new BackupTableDetailsRowMapper());
		return lstBackupTableDetails;
	}

	/**
	 * This method alters existing table to add new impacted column
	 * 
	 * @param query
	 * @return
	 */
	@Transactional
	public boolean alterBackupTable(String[] query) {
		boolean altstatus=false;
		String CURRENT_METHOD = "alterBackupTable";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		System.out.println("alter queries : "+query);
		System.out.println("statt Alter : "+LocalTime.now());
		int[] altcount=jdbcTemplate.batchUpdate(query);
		System.out.println("end of alter : "+LocalTime.now());
		if(altcount!=null && altcount.length>0){
			altstatus=true;
		}
		return altstatus;
	}


	/**
	 * This method is to Truncate backup tables
	 * 
	 * @param list of lstBackupTableDetails
	 * @return true or false
	 */
	@Transactional
	public Boolean refreshBackupTables(List<BackupTableDetails> lstBackupTableDetails) {
		String CURRENT_METHOD = "refreshBackupTables";
		Boolean bkpupRefreshStatus = true;
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		Set<String> setBackupTables = new HashSet();
		setBackupTables = getBackupTableList(lstBackupTableDetails);
		System.out.println("setBackupTables truncate" + setBackupTables);
		setBackupTables = lstBackupTableDetails.stream().map(e -> "GDPR." + e.getBackupTableName().toUpperCase())
				.collect(Collectors.toSet());

		String[] backpQuery = setBackupTables.toArray(new String[setBackupTables.size()]);
		int bkpTableLenth = backpQuery.length;
		for (int i = 0; i < bkpTableLenth; i++) {
			backpQuery[i] = "DELETE FROM " + backpQuery[i];
		}
		System.out.println("statt of truncate essor : " + LocalTime.now());
		System.out.println("before truncate" + Arrays.toString(backpQuery));
        try {
            if (setBackupTables != null && !setBackupTables.isEmpty()) {

				int[] a = jdbcTemplate.batchUpdate(backpQuery);
				System.out.println("After truncate" + a);

			}
			bkpupRefreshStatus = true;
			System.out.println("end of truncate essor : " + LocalTime.now());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			bkpupRefreshStatus = false;
			e.printStackTrace();
		}

		// }
		return bkpupRefreshStatus;
	}

	/****************************************************************************************
	 * This rowMapper converts the row data from BackupTable table to
	 * BackupTableDetails Object
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class BackupTableDetailsRowMapper implements RowMapper {
		private String CURRENT_CLASS = GlobalConstants.CLS_BACKUPTABLEDETAILSROWMAPPER;

		/*
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public BackupTableDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			// System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");

			return new BackupTableDetails(rs.getString("table_name"), rs.getString("column_name"));
		}
	}

	/**
	 * PreparingListofBackupTable Names
	 * 
	 * @return List of BackupTable Names
	 */
	public Set getBackupTableList(List<BackupTableDetails> lstBackupTableColumn) {
		String CURRENT_METHOD = "getBackupTableList";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		Set lstBackupTables = new HashSet();
		int backupTableLnth = lstBackupTableColumn.size();
		for (int i = 0; i < backupTableLnth; i++) {
			String backupTableName = lstBackupTableColumn.get(i).getBackupTableName();

			lstBackupTables.add("GDPR." + backupTableName);

		}

		return lstBackupTables;
	}

}