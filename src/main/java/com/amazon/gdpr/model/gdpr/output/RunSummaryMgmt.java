package com.amazon.gdpr.model.gdpr.output;

public class RunSummaryMgmt {
	
	long summaryId;
	long runId;
	int categoryId;
	String region;
	String countryCode;
	int impactTableId;
	String impactTableName;
	String backupQuery;
	String depersonalizationQuery;
	String taggedQueryLoad;
	long backupRowCount;
	long taggedRowCount;
	long depersonalizedRowCount;
	
	public RunSummaryMgmt(){
		
	}
	
	/**
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param impactTableName
	 * @param backupQuery
	 * @param depersonalizationQuery
	 */
	public RunSummaryMgmt(long runId, int categoryId, String region, String countryCode, int impactTableId, String impactTableName,
			String backupQuery, String depersonalizationQuery) {
		super();		
		this.runId = runId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.impactTableId = impactTableId;
		this.impactTableName = impactTableName;
		this.backupQuery = backupQuery;
		this.depersonalizationQuery = depersonalizationQuery;
	}
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param impactTableName
	 * @param backupQuery
	 * @param depersonalizationQuery
	 */
	public RunSummaryMgmt(long summaryId, long runId, int categoryId, String countryCode, int impactTableId,
			String impactTableName, String backupQuery, String depersonalizationQuery, String taggedQueryLoad) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		this.categoryId = categoryId;
		this.countryCode = countryCode;
		this.impactTableId = impactTableId;
		this.impactTableName = impactTableName;
		this.backupQuery = backupQuery;
		this.depersonalizationQuery = depersonalizationQuery;
		this.taggedQueryLoad = taggedQueryLoad;
	}
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param impactTableName
	 * @param backupQuery
	 * @param depersonalizationQuery
	 * @param backupRowCount
	 * @param taggedRowCount
	 * @param depersonalizedRowCount
	 */
	public RunSummaryMgmt(long summaryId, long runId, int categoryId, String region, String countryCode, int impactTableId,
			String impactTableName, String backupQuery, String depersonalizationQuery, long backupRowCount, long taggedRowCount,
			long depersonalizedRowCount) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.impactTableId = impactTableId;
		this.impactTableName = impactTableName;
		this.backupQuery = backupQuery;
		this.depersonalizationQuery = depersonalizationQuery;
		this.backupRowCount = backupRowCount;
		this.taggedRowCount = taggedRowCount;
		this.depersonalizedRowCount = depersonalizedRowCount;
	}

	/**
	 * @return the summaryId
	 */
	public long getSummaryId() {
		return summaryId;
	}

	/**
	 * @param summaryId the summaryId to set
	 */
	public void setSummaryId(long summaryId) {
		this.summaryId = summaryId;
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
	 * @return the region
	 */
	public String getRegion() {
		return region;
	}

	/**
	 * @param region the region to set
	 */
	public void setRegion(String region) {
		this.region = region;
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
	 * @return the impactTableId
	 */
	public int getImpactTableId() {
		return impactTableId;
	}

	/**
	 * @param impactTableId the impactTableId to set
	 */
	public void setImpactTableId(int impactTableId) {
		this.impactTableId = impactTableId;
	}

	/**
	 * @return the impactTableName
	 */
	public String getImpactTableName() {
		return impactTableName;
	}

	/**
	 * @param impactTableName the impactTableName to set
	 */
	public void setImpactTableName(String impactTableName) {
		this.impactTableName = impactTableName;
	}

	/**
	 * @return the backupQuery
	 */
	public String getBackupQuery() {
		return backupQuery;
	}

	/**
	 * @param backupQuery the backupQuery to set
	 */
	public void setBackupQuery(String backupQuery) {
		this.backupQuery = backupQuery;
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
	 * @return the taggedQueryLoad
	 */
	public String getTaggedQueryLoad() {
		return taggedQueryLoad;
	}

	/**
	 * @param taggedQueryLoad the taggedQueryLoad to set
	 */
	public void setTaggedQueryLoad(String taggedQueryLoad) {
		this.taggedQueryLoad = taggedQueryLoad;
	}

	/**
	 * @return the backupRowCount
	 */
	public long getBackupRowCount() {
		return backupRowCount;
	}

	/**
	 * @param backupRowCount the backupRowCount to set
	 */
	public void setBackupRowCount(long backupRowCount) {
		this.backupRowCount = backupRowCount;
	}

	/**
	 * @return the taggedRowCount
	 */
	public long getTaggedRowCount() {
		return taggedRowCount;
	}

	/**
	 * @param taggedRowCount the taggedRowCount to set
	 */
	public void setTaggedRowCount(long taggedRowCount) {
		this.taggedRowCount = taggedRowCount;
	}

	/**
	 * @return the depersonalizedRowCount
	 */
	public long getDepersonalizedRowCount() {
		return depersonalizedRowCount;
	}

	/**
	 * @param depersonalizedRowCount the depersonalizedRowCount to set
	 */
	public void setDepersonalizedRowCount(long depersonalizedRowCount) {
		this.depersonalizedRowCount = depersonalizedRowCount;
	}
	
	@Override
	public String toString() {
		return (this.runId +" "+this.categoryId +" "+" "+this.region+" "+this.countryCode+" "+this.impactTableId+" "
					+this.backupQuery+" "+this.depersonalizationQuery);
	}
}