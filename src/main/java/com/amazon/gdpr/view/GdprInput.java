package com.amazon.gdpr.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This bean is loaded when the GDPR Depersonalization is loaded in the UI 
 * This carries data back and forth from Controller to UI when page loads 
 * and UI to Controller when form is submitted 
 ****************************************************************************************/
public class GdprInput {
		
	private String runStatus;
	private String errorStatus;
	private String runName;
	private String countries;
	private List<String> lstRegion;
	private List<String> lstCountry;
	private Map<String, List<String>> mapRegionCountry;	
	private String[] arySelectedRegion;	
	private String[] arySelectedCountryCode;
	private List<String> lstSelectedCountry;
 	
	public GdprInput() {
		errorStatus = GlobalConstants.EMPTY_STRING;
		lstRegion = new ArrayList<String>();
		lstCountry = new ArrayList<String>();
		mapRegionCountry = new HashMap<String, List<String>>();
		arySelectedRegion = new String[0];
		arySelectedCountryCode = new String[0];
		lstSelectedCountry = new ArrayList<String>();
	}
	
	/**
	 * This status will be forwarded to the UI and displayed
	 * @return the status displaying the success or failure of the run
	 */
	public String getRunStatus() {
		return runStatus;
	}

	/**
	 * This form detail will be processed and the status will be set in the processing page
	 * @param runStatus 
	 */
	public void setRunStatus(String runStatus) {
		this.runStatus = runStatus;
	}	

	/**
	 * @return the errorStatus
	 */
	public String getErrorStatus() {
		return errorStatus;
	}

	/**
	 * @param errorStatus the errorStatus to set
	 */
	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}

	/**
	 * @return the runName
	 */
	public String getRunName() {
		return runName;
	}

	/**
	 * @param runName the runName to set
	 */
	public void setRunName(String runName) {
		this.runName = runName;
	}

	/**
	 * @return the countries
	 */
	public String getCountries() {
		return countries;
	}

	/**
	 * @param countries the countries to set
	 */
	public void setCountries(String countries) {
		this.countries = countries;
	}

	/**
	 * @return the lstRegion
	 */
	public List<String> getLstRegion() {
		return lstRegion;
	}

	/**
	 * @param lstRegion the lstRegion to set
	 */
	public void setLstRegion(List<String> lstRegion) {
		this.lstRegion = lstRegion;
	}

	/**
	 * @return the lstCountry
	 */
	public List<String> getLstCountry() {
		return lstCountry;
	}

	/**
	 * @param lstCountry the lstCountry to set
	 */
	public void setLstCountry(List<String> lstCountry) {
		this.lstCountry = lstCountry;
	}

	/**
	 * @return the mapRegionCountry
	 */
	public Map<String, List<String>> getMapRegionCountry() {
		return mapRegionCountry;
	}

	/**
	 * @param mapRegionCountry the mapRegionCountry to set
	 */
	public void setMapRegionCountry(Map<String, List<String>> mapRegionCountry) {
		this.mapRegionCountry = mapRegionCountry;
	}

	/**
	 * @return the arySelectedRegion
	 */
	public String[] getArySelectedRegion() {
		return arySelectedRegion;
	}

	/**
	 * @param arySelectedRegion the arySelectedRegion to set
	 */
	public void setArySelectedRegion(String[] arySelectedRegion) {
		this.arySelectedRegion = arySelectedRegion;
	}

	/**
	 * @return the arySelectedCountryCode
	 */
	public String[] getArySelectedCountryCode() {
		return arySelectedCountryCode;
	}

	/**
	 * @param arySelectedCountryCode the arySelectedCountryCode to set
	 */
	public void setArySelectedCountryCode(String[] arySelectedCountryCode) {
		this.arySelectedCountryCode = arySelectedCountryCode;
	}

	/**
	 * @return the lstSelectedCountry
	 */
	public List<String> getLstSelectedCountry() {
		return lstSelectedCountry;
	}

	/**
	 * @param lstSelectedCountry the lstSelectedCountry to set
	 */
	public void setLstSelectedCountry(List<String> lstSelectedCountry) {
		this.lstSelectedCountry = lstSelectedCountry;
	}
}