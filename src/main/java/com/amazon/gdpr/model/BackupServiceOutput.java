package com.amazon.gdpr.model;

public class BackupServiceOutput {
	long summaryId;
	long runId;
	long backupRowCount;
	
	
	public BackupServiceOutput(){
		
	}
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param backupRowCount
	 */
	public BackupServiceOutput(long summaryId,long runId, long backupRowCount) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		
		this.backupRowCount = backupRowCount;
		
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

	
	@Override
	public String toString() {
		return (this.summaryId+" "+this.runId +" "+this.backupRowCount );
	}
}