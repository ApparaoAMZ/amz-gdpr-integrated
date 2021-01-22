package com.amazon.gdpr.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.input.AnonymizationDetail;
import com.amazon.gdpr.model.gdpr.input.Category;
import com.amazon.gdpr.model.gdpr.input.ImpactField;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.input.ImpactTableDetails;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to fetch the Input table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class GdprInputDaoImpl {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRINPUTDAOIMPL;
	private Map<String, String> mapFieldCategory = null; 
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
		
	/**
	 * Fetches the Category Table rows
	 * @return Map <CategoryName, CategoryId>
	 */
	public Map<String, String> fetchCategoryDetails(){		
		String CURRENT_METHOD = "fetchCategoryDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		Map<String, String> mapCategory = null;
					
		@SuppressWarnings("unchecked")
		List<Category> lstCategory = jdbcTemplate.query(SqlQueriesConstant.CATEGORY_FETCH, new CategoryRowMapper());				
		
		if(lstCategory != null && lstCategory.size() > 0){
			mapCategory = new HashMap<String, String>();
			mapFieldCategory = new HashMap<String, String>(); 
			for(Category category : lstCategory) {
				mapCategory.put(category.getCategoryName(), String.valueOf(category.getCategoryId()));
				if(category.getGdprCategoryStatusField() != null && !(category.getGdprCategoryStatusField().equalsIgnoreCase(GlobalConstants.EMPTY_STRING)))
					mapFieldCategory.put(category.getGdprCategoryStatusField(), String.valueOf(category.getCategoryId()));
			}
		}		
		return mapCategory;
	}
	
	/**
	 * Fetches the ImpactTable Table rows
	 * @return List of ImpactTable
	 */
	public List<ImpactTable> fetchImpactTable(){		
		String CURRENT_METHOD = "fetchImpactTable";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<ImpactTable> lstImpactTable = jdbcTemplate.query(SqlQueriesConstant.IMPACT_TABLE_FETCH, new ImpactTableRowMapper());
		return lstImpactTable;
	}
	
	/**
	 * Fetches the Impact Table rows in map
	 * @return Map <IMPACT_TABLE_NAME, IMPACT_TABLE_ID>
	 */
	public Map<String, String> fetchImpactTableMap(String mapType){
		String CURRENT_METHOD = "fetchImpactTableMap";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		Map<String, String> mapTableNameToId = null;
		Map<String, String> mapTableIdToName = null;
		
		@SuppressWarnings("unchecked")
		List<ImpactTable> lstImpactTable = jdbcTemplate.query(SqlQueriesConstant.IMPACT_TABLE_FETCH, new ImpactTableRowMapper());
		
		if(lstImpactTable != null && lstImpactTable.size() > 0){
			mapTableNameToId = new HashMap<String, String>();
			mapTableIdToName = new HashMap<String, String>();
			for(ImpactTable impactTable : lstImpactTable) {			
				mapTableNameToId.put(impactTable.getImpactTableName(), String.valueOf(impactTable.getImpactTableId()));
				mapTableIdToName.put(String.valueOf(impactTable.getImpactTableId()), impactTable.getImpactTableName());
			}
		}
		if(GlobalConstants.IMPACTTABLE_MAP_IDTONAME.equalsIgnoreCase(mapType))
			return mapTableIdToName;
		else
			return mapTableNameToId;
	}
	
	
	
	/**
	 * Fetches the ImpactField Table rows
	 * @return List of ImpactField
	 */
	public List<ImpactField> fetchImpactField(){
		String CURRENT_METHOD = "fetchImpactField";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<ImpactField> lstImpactField = jdbcTemplate.query(SqlQueriesConstant.IMPACT_FIELD_FETCH, new ImpactFieldRowMapper());		
		return lstImpactField;
	}
		
	/**
	 * Fetches the ImpactField rows in map
	 * @return Map <IMPACT_FIELD_NAME, IMPACT_FIELD_ID>
	 */
	public Map<String, String> fetchImpactFieldMap(){	
		String CURRENT_METHOD = "fetchImpactFieldMap";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		Map<String, String> mapImpactField = null;
		List<ImpactField> lstImpactField = fetchImpactField();
		if(lstImpactField != null && lstImpactField.size() > 0){
			mapImpactField = new HashMap<String, String>();
			for(ImpactField impactField : lstImpactField) {
				mapImpactField.put(impactField.getImpactTableId() + impactField.getImpactFieldName(), String.valueOf(impactField.getImpactFieldId()));
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: impactField "+impactField.toString());
			}
		}
		return mapImpactField;		
	}
	/*****Backup Table Processor Code Change Starts***/
	/**
	 * Fetches the ImpactTable Details map
	 * @return list of ImpactTableDetails 
	 */
	public List<ImpactTableDetails>  fetchImpactTableDetailsMap(){	
		String CURRENT_METHOD = "fetchImpactTableDetailsMap";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		@SuppressWarnings("unchecked")
		List<ImpactTableDetails> lstImpactTableDetails = jdbcTemplate.query(SqlQueriesConstant.IMPACTTABLE_DETAILS_QRY, new ImpactTableDetailsRowMapper());
		return lstImpactTableDetails;
		
	}
	/*****Backup Table Processor Code Change Ends***/
	/**
	 * Fetches the AnonymizationDetails Table rows
	 * @return List of AnonymizationDetails
	 */
	public List<AnonymizationDetail> fetchAnonymizationDetails(){		
		String CURRENT_METHOD = "fetchAnonymizationDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<AnonymizationDetail> lstAnonymizationDetail = jdbcTemplate.query(SqlQueriesConstant.ANONYMIZATION_DETAIL_FETCH, 
				new AnonymizationDetailRowMapper());
		return lstAnonymizationDetail;
	}
	
	/**
	 * This method inserts the ImpactField values 
	 * @param lstImpactField
	 * @param batchSize
	 * @return
	 */
	@Transactional
	public void batchInsertImpactField(List<ImpactField> lstImpactField, int batchSize) {
		String CURRENT_METHOD = "batchInsertImpactField";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		jdbcTemplate.batchUpdate(SqlQueriesConstant.IMPACT_FIELD_INSERT, lstImpactField, batchSize, 
						new ParameterizedPreparedStatementSetter<ImpactField>() {
			public void setValues(PreparedStatement ps, ImpactField impactField) throws SQLException {
				ps.setInt(1, impactField.getImpactTableId());
				ps.setString(2, impactField.getImpactFieldLabel());
				ps.setString(3, impactField.getImpactFieldName());
				ps.setString(4, impactField.getImpactFieldType());				
			}
		});
	}
	
	/**
	 * This method inserts the AnonymizationDetail values 
	 * @param lstImpactField
	 * @param batchSize
	 * @return
	 */
	@Transactional
	public void batchInsertAnonymizationDetail(List<AnonymizationDetail> lstAnonymizationDetail, int batchSize) {
		String CURRENT_METHOD = "batchInsertAnonymizationDetail";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[][] insertCounts = jdbcTemplate.batchUpdate(SqlQueriesConstant.ANONYMIZATION_DETAIL_INSERT, lstAnonymizationDetail, batchSize, 
						new ParameterizedPreparedStatementSetter<AnonymizationDetail>() {
			public void setValues(PreparedStatement ps, AnonymizationDetail anonymizationDetail) throws SQLException {
				
				ps.setInt(1, anonymizationDetail.getImpactFieldId());
				ps.setInt(2, anonymizationDetail.getCategoryId());				
				ps.setString(3, anonymizationDetail.getRegion());
				ps.setString(4, anonymizationDetail.getCountryCode());
				ps.setString(5, anonymizationDetail.getTransformation());
				ps.setString(6, anonymizationDetail.getStatus());
								
			}
		});
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: insertCounts"+insertCounts);
	}
	
	
	/****************************************************************************************
	 * This rowMapper converts the row data from Category table to Category Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class CategoryRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_CATEGORYROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public Category mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
						
			return new Category(rs.getInt("CATEGORY_ID"), rs.getString("CATEGORY_NAME"), rs.getString("GDPR_CATEGORY_STATUS_FIELD"), rs.getString("STATUS"));
		}
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from IMPACTTABLE table to ImpactTable Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ImpactTableRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_IMPACTTABLEROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public ImpactTable mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new ImpactTable(rs.getInt("IMPACT_TABLE_ID"), rs.getString("IMPACT_TABLE_NAME"), rs.getString("PARENT_TABLE_NAME"), 
					rs.getString("IMPACT_TABLE_COLUMN"), rs.getString("PARENT_TABLE_COLUMN"));		
		}
	}
		
	/****************************************************************************************
	 * This rowMapper converts the row data from IMPACTFIELD table to ImpactField Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ImpactFieldRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_IMPACTFIELDROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public ImpactField mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new ImpactField(rs.getInt("IMPACT_FIELD_ID"), rs.getInt("IMPACT_TABLE_ID"), rs.getString("IMPACT_FIELD_LABEL"),   
					rs.getString("IMPACT_FIELD_NAME"), rs.getString("IMPACT_FIELD_TYPE"));			
		}
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from ANONYMIZATION_DETAIL table to AnonymizationDetail Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class AnonymizationDetailRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZATIONDETAILROwMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public AnonymizationDetail mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new AnonymizationDetail(rs.getInt("IMPACT_FIELD_ID"), rs.getInt("CATEGORY_ID"),   
					rs.getString("REGION"), rs.getString("COUNTRY_CODE").trim(), rs.getString("TRANSFORMATION_TYPE"), rs.getString("STATUS"));		
		}
	}
	/*****Backup Table Processor Code Change Starts***/
	/****************************************************************************************
	 * This rowMapper converts the row data from ImpactDetails  to ImpactTableDetails Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ImpactTableDetailsRowMapper implements RowMapper{		
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_IMPACTTABLEDETAILSROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public ImpactTableDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
						
			return new ImpactTableDetails(rs.getString("IMPACT_TABLE_NAME"), rs.getString("IMPACT_FIELD_NAME"), rs.getString("IMPACT_FIELD_TYPE"));
		}
	}
	/*****Backup Table Processor Code Change Ends***/
	/**
	 * @return the mapFieldCategory
	 */
	public Map<String, String> getMapFieldCategory() {
		return mapFieldCategory;
	}

	/**
	 * @param mapFieldCategory the mapFieldCategory to set
	 */
	public void setMapFieldCategory(Map<String, String> mapFieldCategory) {
		this.mapFieldCategory = mapFieldCategory;
	}	
}