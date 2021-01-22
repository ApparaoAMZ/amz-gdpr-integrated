package com.amazon.gdpr.model.gdpr.output;

public class RunAnonymization {
	
	long runAnonymizationId;
	long runId;
	int anonymizationDetailId;
	
	public RunAnonymization(long runId, int anonymizationDetailId) {
		this.runId = runId;
		this.anonymizationDetailId = anonymizationDetailId;
	}
	
	/**
	 * @param mappingId
	 * @param runId
	 * @param anonymizationId
	 */
	public RunAnonymization(long runAnonymizationId, long runId, int anonymizationDetailId) {
		super();
		this.runAnonymizationId = runAnonymizationId;
		this.runId = runId;
		this.anonymizationDetailId = anonymizationDetailId;
	}
	
	/**
	 * @return the mappingId
	 */
	public long getrunAnonymizationId() {
		return runAnonymizationId;
	}
	
	/**
	 * @param mappingId the mappingId to set
	 */
	public void setrunAnonymizationId(long runAnonymizationId) {
		this.runAnonymizationId = runAnonymizationId;
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
	 * @return the anonymizationId
	 */
	public int getAnonymizationDetailId() {
		return anonymizationDetailId;
	}
	/**
	 * @param anonymizationDetailId the anonymizationDetailId to set
	 */
	public void setAnonymizationDetailId(int anonymizationDetailId) {
		this.anonymizationDetailId = anonymizationDetailId;
	}
	
}