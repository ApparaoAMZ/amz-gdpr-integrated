package com.amazon.gdpr.model.gdpr.input;

public class ImpactField {

	int impactFieldId;
	int impactTableId;
	String impactFieldLabel;
	String impactFieldName;
	String impactFieldType;
	
	public ImpactField() {
		
	}
	
	public ImpactField(int impactTableId, String impactFieldLabel, String impactFieldName,
			String impactFieldType) {
		this.impactTableId = impactTableId;
		this.impactFieldLabel = impactFieldLabel;
		this.impactFieldName = impactFieldName;
		this.impactFieldType = impactFieldType;
	}	
	
	/**
	 * @param impactFieldId
	 * @param impactTableId
	 * @param impactFieldLabel
	 * @param impactFieldName
	 * @param impactFieldType
	 */
	public ImpactField(int impactFieldId, int impactTableId, String impactFieldLabel, String impactFieldName,
			String impactFieldType) {
		super();
		this.impactFieldId = impactFieldId;
		this.impactTableId = impactTableId;
		this.impactFieldLabel = impactFieldLabel;
		this.impactFieldName = impactFieldName;
		this.impactFieldType = impactFieldType;
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
	 * @return the impactFieldLabel
	 */
	public String getImpactFieldLabel() {
		return impactFieldLabel;
	}
	/**
	 * @param impactFieldLabel the impactFieldLabel to set
	 */
	public void setImpactFieldLabel(String impactFieldLabel) {
		this.impactFieldLabel = impactFieldLabel;
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
	
	@Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if(obj instanceof ImpactField)
        {
        	ImpactField temp = (ImpactField) obj;        	        
        	if(this.impactTableId == temp.impactTableId && this.impactFieldLabel.equalsIgnoreCase(temp.impactFieldLabel) && 
        		this.impactFieldName.equalsIgnoreCase(temp.impactFieldName) && 
        		this.impactFieldType.equalsIgnoreCase(temp.impactFieldType))        		
                return true;
        }
        return false;
    }
    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
    	return (String.valueOf(this.impactTableId).hashCode() + this.impactFieldLabel.hashCode() + 
    			this.impactFieldName.hashCode() + this.impactFieldType.hashCode());    	    			        
    }
    
    @Override
    public String toString() {    	
    	return this.getImpactTableId() +" "+ this.getImpactFieldName() +" "+ this.getImpactFieldId();
    }
}