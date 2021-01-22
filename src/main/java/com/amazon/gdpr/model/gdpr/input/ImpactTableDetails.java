package com.amazon.gdpr.model.gdpr.input;

public class ImpactTableDetails {

	String impactTableName;
	String impactColumnName;
	String impactColumnType;
	
	/**
	 * @param impactTableName
	 * @param impactColumnName
	 */
	public ImpactTableDetails(String impactTableName,
			String impactColumnName, String impactColumnType) {
		this.impactTableName = impactTableName;
		this.impactColumnName = impactColumnName;
		this.impactColumnType = impactColumnType;
		
	}

	
	
	
	public String getImpactTableName() {
		return impactTableName;
	}




	public void setImpactTableName(String impactTableName) {
		this.impactTableName = impactTableName;
	}




	public String getImpactColumnName() {
		return impactColumnName;
	}




	public void setImpactColumnName(String impactColumnName) {
		this.impactColumnName = impactColumnName;
	}




	public String getImpactColumnType() {
		return impactColumnType;
	}




	public void setImpactColumnType(String impactColumnType) {
		this.impactColumnType = impactColumnType;
	}




	@Override
	public String toString() {
		return (this.impactTableName  +" "+ this.impactColumnName +" "+ this.impactColumnType);
	}	
}