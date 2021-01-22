package com.amazon.gdpr.model.gdpr.output;

public class SummaryData {

	public int categoryId;
	public String region;
	public String countryCode;
	public String impactSchema;
	public String impactTableName;
	public String impactFieldName;
	public String impactFieldType;
	public String transformationType;
	public int impactTableId;
	
	/**
	 * @param categoryId
	 * @param region
	 * @param countryCode
	 * @param impactTableName
	 * @param impactFieldName
	 * @param transformationType
	 * @param impactTableId
	 */
	public SummaryData(int categoryId, String region, String countryCode, String impactTableName, String impactSchema, 
			String impactFieldName, String impactFieldType, String transformationType, int impactTableId) {
		super();
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.impactSchema = impactSchema;
		this.impactTableName = impactTableName;
		this.impactFieldName = impactFieldName;
		this.impactFieldType = impactFieldType;
		this.transformationType = transformationType;
		this.impactTableId = impactTableId;
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
	 * @return the impactSchema
	 */
	public String getImpactSchema() {
		return impactSchema;
	}

	/**
	 * @param impactSchema the impactSchema to set
	 */
	public void setImpactSchema(String impactSchema) {
		this.impactSchema = impactSchema;
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
	 * @return the impactFieldName
	 */
	public String getImpactFieldName() {
		return impactFieldName;
	}

	/**
	 * @param impactFieldName the impactFieldName to set
	 */
	public void setImpactFieldName(String impactFieldName) {
		this.impactFieldName = impactFieldName;
	}
	
	/**
	 * @return the impactFieldType
	 */
	public String getImpactFieldType() {
		return impactFieldType;
	}

	/**
	 * @param impactFieldType the impactFieldType to set
	 */
	public void setImpactFieldType(String impactFieldType) {
		this.impactFieldType = impactFieldType;
	}

	/**
	 * @return the transformationType
	 */
	public String getTransformationType() {
		return transformationType;
	}

	/**
	 * @param transformationType the transformationType to set
	 */
	public void setTransformationType(String transformationType) {
		this.transformationType = transformationType;
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
	
}