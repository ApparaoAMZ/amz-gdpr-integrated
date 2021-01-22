package com.amazon.gdpr.model.gdpr.output;

import java.util.Date;

public class RunModuleMgmt {

	long moduleId;
	long runId;
	String moduleName;
	String subModuleName;
	String moduleStatus;
	Date moduleStartDateTime;
	Date moduleEndDateTime;
	String comments;
	String errorDetails;
	int count;
	
	public RunModuleMgmt(){
		
	}
	
	public RunModuleMgmt(String moduleStatus){
		this.moduleStatus = moduleStatus;
	}
	
	/**
	 * @param moduleId
	 * @param runId
	 * @param moduleName
	 * @param moduleStatus
	 * @param moduleStartDateTime
	 * @param moduleEndDateTime
	 * @param comments
	 */
	public RunModuleMgmt(long runId, String moduleName, String subModuleName, String moduleStatus, Date moduleStartDateTime,
			Date moduleEndDateTime, String comments) {
		super();		
		this.runId = runId;
		this.moduleName = moduleName;
		this.subModuleName = subModuleName;
		this.moduleStatus = moduleStatus;
		this.moduleStartDateTime = moduleStartDateTime;
		this.moduleEndDateTime = moduleEndDateTime;
		this.comments = comments;
	}
	
	
	/**
	 * @param moduleId
	 * @param runId
	 * @param moduleName
	 * @param moduleStatus
	 * @param moduleStartDateTime
	 * @param moduleEndDateTime
	 * @param comments
	 * @param errorDetails
	 */
	public RunModuleMgmt(long runId, String moduleName, String subModuleName, String moduleStatus, Date moduleStartDateTime,
			Date moduleEndDateTime, String comments, String errorDetails) {
		super();		
		this.runId = runId;
		this.moduleName = moduleName;
		this.subModuleName = subModuleName;
		this.moduleStatus = moduleStatus;
		this.moduleStartDateTime = moduleStartDateTime;
		this.moduleEndDateTime = moduleEndDateTime;
		this.comments = comments;
		this.errorDetails = errorDetails;
	}

	/**
	 * @return the moduleId
	 */
	public long getModuleId() {
		return moduleId;
	}

	/**
	 * @param moduleId the moduleId to set
	 */
	public void setModuleId(long moduleId) {
		this.moduleId = moduleId;
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
	 * @return the moduleName
	 */
	public String getModuleName() {
		return moduleName;
	}

	/**
	 * @param moduleName the moduleName to set
	 */
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	/**
	 * @return the processorName
	 */
	public String getSubModuleName() {
		return subModuleName;
	}

	/**
	 * @param processorName the processorName to set
	 */
	public void setSubModuleName(String subModuleName) {
		this.subModuleName = subModuleName;
	}

	/**
	 * @return the moduleStatus
	 */
	public String getModuleStatus() {
		return moduleStatus;
	}

	/**
	 * @param moduleStatus the moduleStatus to set
	 */
	public void setModuleStatus(String moduleStatus) {
		this.moduleStatus = moduleStatus;
	}

	/**
	 * @return the moduleStartDateTime
	 */
	public Date getModuleStartDateTime() {
		return moduleStartDateTime;
	}

	/**
	 * @param moduleStartDateTime the moduleStartDateTime to set
	 */
	public void setModuleStartDateTime(Date moduleStartDateTime) {
		this.moduleStartDateTime = moduleStartDateTime;
	}

	/**
	 * @return the moduleEndDateTime
	 */
	public Date getModuleEndDateTime() {
		return moduleEndDateTime;
	}

	/**
	 * @param moduleEndDateTime the moduleEndDateTime to set
	 */
	public void setModuleEndDateTime(Date moduleEndDateTime) {
		this.moduleEndDateTime = moduleEndDateTime;
	}

	/**
	 * @return the comments
	 */
	public String getComments() {
		return comments;
	}

	/**
	 * @param comments the comments to set
	 */
	public void setComments(String comments) {
		this.comments = comments;
	}
	
	/**
	 * @return the errorDetails
	 */
	public String getErrorDetails() {
		return errorDetails;
	}

	/**
	 * @param errorDetails the errorDetails to set
	 */
	public void setErrorDetails(String errorDetails) {
		this.errorDetails = errorDetails;
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}
}