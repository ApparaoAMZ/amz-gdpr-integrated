package com.amazon.gdpr.model;

public class GdprDepersonalizationInput {
	
	public String candidate;
	public String category;
	public String countryCode;
	
	public String bgcStatus;
	public String amazonAssessmentStatus;
	public String candidateProvidedStatus;
	public String masterDataStatus;
	public String withConsentStatus;
	
	public GdprDepersonalizationInput() {
		
	}
	
	/**
	 * @param candidate
	 * @param category
	 * @param countryCode
	 * @param bgcStatus
	 * @param amazonAssessmentStatus
	 * @param candidateProvidedStatus
	 * @param masterDataStatus
	 * @param withConsentStatus
	 */
	public GdprDepersonalizationInput(String candidate, String category, String countryCode, String bgcStatus,
			String amazonAssessmentStatus, String candidateProvidedStatus, String masterDataStatus,
			String withConsentStatus) {
		super();
		this.candidate = candidate;
		this.category = category;
		this.countryCode = countryCode;
		this.bgcStatus = bgcStatus;
		this.amazonAssessmentStatus = amazonAssessmentStatus;
		this.candidateProvidedStatus = candidateProvidedStatus;
		this.masterDataStatus = masterDataStatus;
		this.withConsentStatus = withConsentStatus;
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
	public String getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(String category) {
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
	 * @return the bgcStatus
	 */
	public String getBgcStatus() {
		return bgcStatus;
	}
	/**
	 * @param bgcStatus the bgcStatus to set
	 */
	public void setBgcStatus(String bgcStatus) {
		this.bgcStatus = bgcStatus;
	}
	/**
	 * @return the amazonAssessmentStatus
	 */
	public String getAmazonAssessmentStatus() {
		return amazonAssessmentStatus;
	}
	/**
	 * @param amazonAssessmentStatus the amazonAssessmentStatus to set
	 */
	public void setAmazonAssessmentStatus(String amazonAssessmentStatus) {
		this.amazonAssessmentStatus = amazonAssessmentStatus;
	}
	/**
	 * @return the candidateProvidedStatus
	 */
	public String getCandidateProvidedStatus() {
		return candidateProvidedStatus;
	}
	/**
	 * @param candidateProvidedStatus the candidateProvidedStatus to set
	 */
	public void setCandidateProvidedStatus(String candidateProvidedStatus) {
		this.candidateProvidedStatus = candidateProvidedStatus;
	}
	/**
	 * @return the masterDataStatus
	 */
	public String getMasterDataStatus() {
		return masterDataStatus;
	}
	/**
	 * @param masterDataStatus the masterDataStatus to set
	 */
	public void setMasterDataStatus(String masterDataStatus) {
		this.masterDataStatus = masterDataStatus;
	}
	/**
	 * @return the withConsentStatus
	 */
	public String getWithConsentStatus() {
		return withConsentStatus;
	}
	/**
	 * @param withConsentStatus the withConsentStatus to set
	 */
	public void setWithConsentStatus(String withConsentStatus) {
		this.withConsentStatus = withConsentStatus;
	}

}