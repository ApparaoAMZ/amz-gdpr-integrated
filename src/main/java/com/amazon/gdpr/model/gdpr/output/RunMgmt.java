package com.amazon.gdpr.model.gdpr.output;

import java.sql.Date;

/****************************************************************************************
 * This model has the structure of the Table - RunMgmt in schema - GDPR 
 ****************************************************************************************/
public class RunMgmt {

	private long runId;
	private String runName;
	private String runStatus;
	private Date runStartDateTime;
	private Date runEndDateTime;
	private String runComments;
	
	public RunMgmt(){
		
	}
	
	public RunMgmt(long runId, String runStatus){
		super();
		this.runId = runId;		
		this.runStatus = runStatus;
	}
	
	/**
	 * @param runId
	 * @param runName
	 * @param runStatus
	 * @param runStartDateTime
	 * @param runEndDateTime
	 * @param runComments
	 */
	public RunMgmt(long runId, String runName, String runStatus, Date runStartDateTime, Date runEndDateTime,
			String runComments) {
		super();
		this.runId = runId;
		this.runName = runName;
		this.runStatus = runStatus;
		this.runStartDateTime = runStartDateTime;
		this.runEndDateTime = runEndDateTime;
		this.runComments = runComments;
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
	 * @return the runStatus
	 */
	public String getRunStatus() {
		return runStatus;
	}

	/**
	 * @param runStatus the runStatus to set
	 */
	public void setRunStatus(String runStatus) {
		this.runStatus = runStatus;
	}

	/**
	 * @return the runStartDateTime
	 */
	public Date getRunStartDateTime() {
		return runStartDateTime;
	}

	/**
	 * @param runStartDateTime the runStartDateTime to set
	 */
	private void setRunStartDateTime(Date runStartDateTime) {
		this.runStartDateTime = runStartDateTime;
	}

	/**
	 * @return the runEndDateTime
	 */
	public Date getRunEndDateTime() {
		return runEndDateTime;
	}

	/**
	 * @param runEndDateTime the runEndDateTime to set
	 */
	private void setRunEndDateTime(Date runEndDateTime) {
		this.runEndDateTime = runEndDateTime;
	}

	/**
	 * @return the runComments
	 */
	public String getRunComments() {
		return runComments;
	}

	/**
	 * @param runComments the runComments to set
	 */
	private void setRunComments(String runComments) {
		this.runComments = runComments;
	}	
}