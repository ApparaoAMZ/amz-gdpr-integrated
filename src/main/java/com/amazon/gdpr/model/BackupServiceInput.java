package com.amazon.gdpr.model;

public class BackupServiceInput {
	
	long summaryId;
	long runId;
	int categoryId;
	String region;
	String countryCode;
	int impactTableId;
	String backupQuery;
	String depersonalizationQuery;
	long backupRowCount;
	long taggedRowCount;
	long depersonalizedRowCount;
	
	public BackupServiceInput(){
		
	}
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param backupQuery
	 * @param depersonalizationQuery
	 * @param backupRowCount
	 * @param taggedRowCount
	 * @param depersonalizedRowCount
	 */
	public BackupServiceInput(long summaryId,long runId, int categoryId, String region, String countryCode, int impactTableId,
			String backupQuery, String depersonalizationQuery) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.impactTableId = impactTableId;
		this.backupQuery = backupQuery;
		this.depersonalizationQuery = depersonalizationQuery;
	}
	
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param backupQuery
	 * @param depersonalizationQuery
	 * @param backupRowCount
	 * @param taggedRowCount
	 * @param depersonalizedRowCount
	 */
	public BackupServiceInput(long summaryId, long runId, int categoryId, String region, String countryCode, int impactTableId,
			String backupQuery, String depersonalizationQuery, long backupRowCount, long taggedRowCount,
			long depersonalizedRowCount) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.impactTableId = impactTableId;
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
		return (this.summaryId+" "+this.runId +" "+this.categoryId +" "+" "+this.region+" "+this.countryCode+" "+this.impactTableId+" "
					+this.backupQuery+" "+this.depersonalizationQuery);
	}
}