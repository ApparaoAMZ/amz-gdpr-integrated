package com.amazon.gdpr.model.archive;

import java.util.List;

/****************************************************************************************
 * This model has the structure of the sf_archive table queries 
 ****************************************************************************************/
public class AnonymizeTable {
	
	private long runId;
	private int tableId;
	private String tableName;
	private String countryCode;	
	private int categoryId;
	private String depersonalizationQuery;
	private int anonymizeId;
	private List<Integer> lstAnonymizeId;

	/**
	 * @param runId
	 * @param tableId
	 * @param tableName
	 * @param countryCode
	 * @param categoryId
	 * @param depersonalizationQuery
	 * @param lstAnonymizeId
	 */
	public AnonymizeTable(long runId, int tableId, String tableName, String countryCode, int categoryId,
			String depersonalizationQuery, int anonymizeId, List<Integer> lstAnonymizeId) {
		super();
		this.runId = runId;
		this.tableId = tableId;
		this.tableName = tableName;
		this.countryCode = countryCode;
		this.categoryId = categoryId;
		this.depersonalizationQuery = depersonalizationQuery;
		this.anonymizeId = anonymizeId;
		this.lstAnonymizeId = lstAnonymizeId;
	}
	/**
	 * @return the runId
	 */
	public long getRunId() {
		return runId;
	}
	/**
	 * @param runId the runId to set
	 */
	public void setRunId(long runId) {
		this.runId = runId;
	}
	/**
	 * @return the tableId
	 */
	public int getTableId() {
		return tableId;
	}
	/**
	 * @param tableId the tableId to set
	 */
	public void setTableId(int tableId) {
		this.tableId = tableId;
	}
	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}
	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	/**
	 * @return the countryCode
	 */
	public String getCountryCode() {
		return countryCode;
	}
	/**
	 * @param countryCode the countryCode to set
	 */
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	/**
	 * @return the categoryId
	 */
	public int getCategoryId() {
		return categoryId;
	}
	/**
	 * @param categoryId the categoryId to set
	 */
	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
	}
	/**
	 * @return the depersonalizationQuery
	 */
	public String getDepersonalizationQuery() {
		return depersonalizationQuery;
	}
	/**
	 * @param depersonalizationQuery the depersonalizationQuery to set
	 */
	public void setDepersonalizationQuery(String depersonalizationQuery) {
		this.depersonalizationQuery = depersonalizationQuery;
	}
	/**
	 * @return the anonymizeId
	 */
	public int getAnonymizeId() {
		return anonymizeId;
	}
	/**
	 * @param anonymizeId the anonymizeId to set
	 */
	public void setAnonymizeId(int anonymizeId) {
		this.anonymizeId = anonymizeId;
	}
	/**
	 * @return the lstAnonymizeId
	 */
	public List<Integer> getLstAnonymizeId() {
		return lstAnonymizeId;
	}
	/**
	 * @param lstAnonymizeId the lstAnonymizeId to set
	 */
	public void setLstAnonymizeId(List<Integer> lstAnonymizeId) {
		this.lstAnonymizeId = lstAnonymizeId;
	}
}