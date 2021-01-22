package com.amazon.gdpr.model;

public class GdprDepersonalizationOutput {
	
	long runId;
	String candidate;
	int category;
	String countryCode;
	
	String hvhStatus;
	String herokuStatus;

	/**
	 * @param candidate
	 * @param category
	 * @param countryCode
	 * @param hvhStatus
	 * @param herokuStatus
	 */
	public GdprDepersonalizationOutput(long runId, String candidate, int category, String countryCode, String hvhStatus,
			String herokuStatus) {
		super();
		this.runId = runId;
		this.candidate = candidate;
		this.category = category;
		this.countryCode = countryCode;
		this.hvhStatus = hvhStatus;
		this.herokuStatus = herokuStatus;
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
	 * @return the candidate
	 */
	public String getCandidate() {
		return candidate;
	}
	/**
	 * @param candidate the candidate to set
	 */
	public void setCandidate(String candidate) {
		this.candidate = candidate;
	}
	/**
	 * @return the category
	 */
	public int getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(int category) {
		this.category = category;
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
	 * @return the hvhStatus
	 */
	public String getHvhStatus() {
		return hvhStatus;
	}
	/**
	 * @param hvhStatus the hvhStatus to set
	 */
	public void setHvhStatus(String hvhStatus) {
		this.hvhStatus = hvhStatus;
	}
	/**
	 * @return the herokuStatus
	 */
	public String getHerokuStatus() {
		return herokuStatus;
	}
	/**
	 * @param herokuStatus the herokuStatus to set
	 */
	public void setHerokuStatus(String herokuStatus) {
		this.herokuStatus = herokuStatus;
	}

}