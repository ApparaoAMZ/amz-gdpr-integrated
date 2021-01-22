package com.amazon.gdpr.model.gdpr.input;

public class Country {

	String region;
	String countryCode;
	String status;

	/**
	 * @param region
	 * @param countryCode
	 */
	public Country(String region, String countryCode) {
		super();
		this.region = region;
		this.countryCode = countryCode;
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