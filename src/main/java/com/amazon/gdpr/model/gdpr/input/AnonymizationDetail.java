package com.amazon.gdpr.model.gdpr.input;

public class AnonymizationDetail {

	int anonymizationDetailId;
	int impactFieldId;
	int categoryId;
	String region;
	String countryCode;
	String transformation;
	String status;
			
	/**
	 * @param impactFieldId
	 * @param categoryId
	 * @param region
	 * @param countryCode
	 * @param transformation
	 * @param status
	 */
	public AnonymizationDetail(int impactFieldId, int categoryId, String region,
			String countryCode, String transformation, String status) {
		this.impactFieldId = impactFieldId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.transformation = transformation;
		this.status = status;
	}

	
	/**
	 * @param anonymizationDetailId
	 * @param impactFieldId
	 * @param categoryId
	 * @param region
	 * @param countryCode
	 * @param transformation
	 * @param status
	 */
	public AnonymizationDetail(int anonymizationDetailId, int impactFieldId, int categoryId, String region,
			String countryCode, String transformation, String status) {
		super();
		this.anonymizationDetailId = anonymizationDetailId;
		this.impactFieldId = impactFieldId;
		this.categoryId = categoryId;
		this.region = region;
		this.countryCode = countryCode;
		this.transformation = transformation;
		this.status = status;
	}

	/**
	 * @return the anonymizationDetailId
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
	/**
	 * @return the impactFieldId
	 */
	public int getImpactFieldId() {
		return impactFieldId;
	}
	/**
	 * @param impactFieldId the impactFieldId to set
	 */
	public void setImpactFieldId(int impactFieldId) {
		this.impactFieldId = impactFieldId;
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
	 * @return the transformation
	 */
	public String getTransformation() {
		return transformation;
	}

	/**
	 * @param transformation the transformation to set
	 */
	public void setTransformation(String transformation) {
		this.transformation = transformation;
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

	@Override
    public boolean equals(Object obj) {
		// TODO Auto-generated method stub
        if(obj instanceof AnonymizationDetail) {
        	AnonymizationDetail temp = (AnonymizationDetail) obj;
        	if(this.impactFieldId == temp.impactFieldId && this.categoryId == temp.categoryId &&
   				(this.region.equalsIgnoreCase(temp.region)) &&
   				(this.countryCode.equalsIgnoreCase(temp.countryCode)) &&
   				(this.transformation.equalsIgnoreCase(temp.transformation)) &&
        		this.status.equalsIgnoreCase(temp.status))
                return true;
        }
        return false;
    }
	
	@Override
    public int hashCode() {
		// TODO Auto-generated method stub
    	return (String.valueOf(this.impactFieldId).hashCode() + String.valueOf(this.categoryId).hashCode() + this.region.hashCode() + 
    			this.countryCode.hashCode() + this.transformation.hashCode() +this.status.hashCode());
    }
	
	@Override
	public String toString() {
		return (this.impactFieldId  +" "+ this.categoryId +" "+ this.region +" "+ this.countryCode +" "+ this.transformation +" "+ this.status);
	}	
}