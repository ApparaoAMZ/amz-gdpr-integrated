package com.amazon.gdpr.view;

public class AnonymizationInputView {

	public String object;
	public String fieldLabel;
	public String apiName;
	public String type;
	public String categoryName;
	public String recommendedTransformation;
	public String chosenTransformation;
	public String region;
	public String countryCode;
	
	public AnonymizationInputView(){		
	}
	
	/**
	 * @param object
	 * @param fieldLabel
	 * @param apiName
	 * @param type
	 * @param categoryName
	 * @param recommendedTransformation
	 * @param chosenTransformation
	 * @param region
	 * @param countryCode
	 */
	public AnonymizationInputView(String object, String fieldLabel, String apiName, String type, String categoryName,
			String recommendedTransformation, String chosenTransformation, String region, String countryCode) {
		super();
		this.object = object;
		this.fieldLabel = fieldLabel;
		this.apiName = apiName;
		this.type = type;
		this.categoryName = categoryName;
		this.recommendedTransformation = recommendedTransformation;
		this.chosenTransformation = chosenTransformation;
		this.region = region;
		this.countryCode = countryCode;
	}
	
	/**
	 * @return the object
	 */
	public String getObject() {
		return object;
	}
	/**
	 * @param object the object to set
	 */
	public void setObject(String object) {
		this.object = object;
	}
	/**
	 * @return the fieldLabel
	 */
	public String getFieldLabel() {
		return fieldLabel;
	}
	/**
	 * @param fieldLabel the fieldLabel to set
	 */
	public void setFieldLabel(String fieldLabel) {
		this.fieldLabel = fieldLabel;
	}
	/**
	 * @return the apiName
	 */
	public String getApiName() {
		return apiName;
	}
	/**
	 * @param apiName the apiName to set
	 */
	public void setApiName(String apiName) {
		this.apiName = apiName;
	}
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
	/**
	 * @return the categoryName
	 */
	public String getCategoryName() {
		return categoryName;
	}
	/**
	 * @param categoryName the categoryName to set
	 */
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	/**
	 * @return the recommendedTransformation
	 */
	public String getRecommendedTransformation() {
		return recommendedTransformation;
	}
	/**
	 * @param recommendedTransformation the recommendedTransformation to set
	 */
	public void setRecommendedTransformation(String recommendedTransformation) {
		this.recommendedTransformation = recommendedTransformation;
	}
	/**
	 * @return the chosenTransformation
	 */
	public String getChosenTransformation() {
		return chosenTransformation;
	}
	/**
	 * @param chosenTransformation the chosenTransformation to set
	 */
	public void setChosenTransformation(String chosenTransformation) {
		this.chosenTransformation = chosenTransformation;
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

	@Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if(obj instanceof AnonymizationInputView) {
        	AnonymizationInputView temp = (AnonymizationInputView) obj;
        	if(this.object.equalsIgnoreCase(temp.object) && this.fieldLabel.equalsIgnoreCase(temp.fieldLabel) && 
        			this.apiName.equalsIgnoreCase(temp.apiName) && this.type.equalsIgnoreCase(temp.type) && 
        			this.categoryName.equalsIgnoreCase(temp.categoryName) && 
        			this.recommendedTransformation.equalsIgnoreCase(temp.recommendedTransformation) &&
        			this.chosenTransformation.equalsIgnoreCase(temp.chosenTransformation) && 
        			(this.region == null || temp.region == null || this.region.equalsIgnoreCase(temp.region)) &&
        			(this.countryCode == null || temp.countryCode == null || this.countryCode.equalsIgnoreCase(temp.countryCode))
        			)
                return true;
        }
        return false;
    }
	
    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
    	int emptyHashCode = String.valueOf("").hashCode();
    	return (this.object.hashCode() + this.fieldLabel.hashCode() + this.apiName.hashCode() + this.type.hashCode() + 
    			this.categoryName.hashCode() +
    			this.recommendedTransformation.hashCode() + this.chosenTransformation.hashCode() + 
    			((this.region != null ) ? this.region.hashCode() : emptyHashCode) + 
    			((this.countryCode != null) ? this.countryCode.hashCode() :emptyHashCode));    			    			      
    }
}