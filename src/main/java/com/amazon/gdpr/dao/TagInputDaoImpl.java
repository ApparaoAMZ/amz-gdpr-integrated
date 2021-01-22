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

import com.amazon.gdpr.model.GdprDepersonalizationOutput;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to load the tag input table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class TagInputDaoImpl {
private static String CURRENT_CLASS		 		= GlobalConstants.CLS_TAGINPUTDAOIMPL;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * This method inserts the Tag values 
	 */
	@Transactional
	public void batchInsertTagTables(Object arg) {
		String CURRENT_METHOD = "batchInsertTagTables";		
		//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		/*int[] insertCounts = jdbcTemplate.batchUpdate(SqlQueriesConstant.GDPR_DEPERSONALIZATION_INPUT, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				GdprDepersonalizationOutput gdprDepersonalizationOutput = (GdprDepersonalizationOutput) lstGdprDepersonalizationOutput.get(i);
				ps.setString(1, gdprDepersonalizationOutput.getCandidate());
				ps.setInt(2, gdprDepersonalizationOutput.getCategory());
				ps.setString(3, gdprDepersonalizationOutput.getCountryCode());
				ps.setString(4, gdprDepersonalizationOutput.getHvhStatus());
				ps.setString(5, gdprDepersonalizationOutput.getHerokuStatus());
				
			}
			public int getBatchSize() {
				return arg.size();
			}
		});*/
		//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: insertCounts"+insertCounts);
	}
	
}