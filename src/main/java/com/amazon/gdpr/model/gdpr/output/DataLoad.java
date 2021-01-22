package com.amazon.gdpr.model.gdpr.output;

import java.util.Date;

/****************************************************************************************
 * This model has the structure of the Table - DATA_LOAD in schema - GDPR 
 ****************************************************************************************/
public class DataLoad {
	
	private String tableName;
	private String countryCode;
	private Date dataLoadDateTime;
	private String strDataLoadDate;
	
	/**
	 * @param tableName
	 * @param countryCode
	 * @param dataLoadDateTime
	 */
	public DataLoad(String tableName, String countryCode, Date dataLoadDateTime) {
		super();
		this.tableName = tableName;
		this.countryCode = countryCode;
		this.dataLoadDateTime = dataLoadDateTime;
	}
	
	/**
	 * @param dataLoadDateTime
	 */
	public DataLoad(String strDataLoadDate) {
		super();
		this.strDataLoadDate = strDataLoadDate;
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
	 * @return the dataLoadDateTime
	 */
	public Date getDataLoadDateTime() {
		return dataLoadDateTime;
	}
	/**
	 * @param dataLoadDateTime the dataLoadDateTime to set
	 */
	public void setDataLoadDateTime(Date dataLoadDateTime) {
		this.dataLoadDateTime = dataLoadDateTime;
	}
	/**
	 * @return the strDataLoadDate
	 */
	public String getStrDataLoadDate() {
		return strDataLoadDate;
	}
	/**
	 * @param strDataLoadDate the strDataLoadDate to set
	 */
	public void setStrDataLoadDate(String strDataLoadDate) {
		this.strDataLoadDate = strDataLoadDate;
	}
}