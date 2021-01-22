package com.amazon.gdpr.model.gdpr.input;

public class Category {
	
	int categoryId;
	String categoryName;
	String gdprCategoryStatusField;
	String status;
	
	public Category(){
		
	}	

	/**
	 * @param categoryId
	 * @param categoryName
	 * @param status
	 */
	public Category(int categoryId, String categoryName, String gdprCategoryStatusField, String status) {
		super();
		this.categoryId = categoryId;
		this.categoryName = categoryName;
		this.gdprCategoryStatusField = gdprCategoryStatusField;
		this.status = status;
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
	 * @return the gdprCategoryStatusField
	 */
	public String getGdprCategoryStatusField() {
		return gdprCategoryStatusField;
	}

	/**
	 * @param gdprCategoryStatusField the gdprCategoryStatusField to set
	 */
	public void setGdprCategoryStatusField(String gdprCategoryStatusField) {
		this.gdprCategoryStatusField = gdprCategoryStatusField;
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
	
}