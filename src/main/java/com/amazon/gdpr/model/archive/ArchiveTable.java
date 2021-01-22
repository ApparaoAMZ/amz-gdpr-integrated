package com.amazon.gdpr.model.archive;

/****************************************************************************************
 * This model has the structure of the Table - Application in schema - sf_archive 
 ****************************************************************************************/
public class ArchiveTable {
	private long runId;
	private int tableId;
	private String tableName;
	private long id;
	private String countryCode;	
	private int categoryId;
	private String status;

	/**
	 * @param runId
	 * @param tableName
	 * @param id
	 * @param countryCode
	 * @param categoryId
	 * @param status
	 */
	public ArchiveTable(long runId, String tableName, long id, String countryCode, int categoryId, String status) {
		super();
		this.runId = runId;
		this.tableName = tableName;
		this.id = id;
		this.countryCode = countryCode;
		this.categoryId = categoryId;
		this.status = status;
	}

	/**
	 * @param runId
	 * @param tableName
	 * @param id
	 * @param countryCode
	 * @param categoryId
	 * @param status
	 */
	public ArchiveTable(long runId, int tableId, long id, String countryCode, int categoryId, String status) {
		super();
		this.runId = runId;
		this.tableId = tableId;
		this.id = id;
		this.countryCode = countryCode;
		this.categoryId = categoryId;
		this.status = status;
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
	 * @return the id
	 */
	public long getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(long id) {
		this.id = id;
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
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}
	/**
	 * @param status the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}
}