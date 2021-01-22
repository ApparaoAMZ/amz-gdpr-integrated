package com.amazon.gdpr.model.gdpr.output;

import com.amazon.gdpr.util.GlobalConstants;

public class RunErrorMgmt {

	long errorId;
	long runId;
	String errorClass;
	String errorMethod;
	String errorSummary;
	String errorDetail;
	
	/**
	 * @param runId
	 * @param errorClass
	 * @param errorMethod
	 * @param errorSummary
	 * @param errorDetail
	 */
	public RunErrorMgmt(long runId, String errorClass, String errorMethod, String errorSummary, String errorDetail) {
		this.runId = runId;
		this.errorClass = errorClass;
		this.errorMethod = errorMethod;
		this.errorSummary = errorSummary;
		if(errorDetail != null && errorDetail.length() > GlobalConstants.ERROR_DETAIL_SIZE)
			this.errorDetail = errorDetail.substring(GlobalConstants.ERROR_INITIAL_INDEX, GlobalConstants.ERROR_DETAIL_SIZE);
		else
			this.errorDetail = errorDetail;
	}
	
	/**
	 * @param errorId
	 * @param runId
	 * @param errorClass
	 * @param errorMethod
	 * @param errorSummary
	 * @param errorDetail
	 */
	public RunErrorMgmt(long errorId, long runId, String errorClass, String errorMethod, String errorSummary,
			String errorDetail) {
		super();
		this.errorId = errorId;
		this.runId = runId;
		this.errorClass = errorClass;
		this.errorMethod = errorMethod;
		this.errorSummary = errorSummary;		
		if(errorDetail != null && errorDetail.length() > GlobalConstants.ERROR_DETAIL_SIZE)
			this.errorDetail = errorDetail.substring(GlobalConstants.ERROR_INITIAL_INDEX, GlobalConstants.ERROR_DETAIL_SIZE);
		else
			this.errorDetail = errorDetail;
	}
	
	/**
	 * @return the errorId
	 */
	public long getErrorId() {
		return errorId;
	}
	/**
	 * @param errorId the errorId to set
	 */
	public void setErrorId(long errorId) {
		this.errorId = errorId;
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
	 * @return the errorClass
	 */
	public String getErrorClass() {
		return errorClass;
	}
	/**
	 * @param errorClass the errorClass to set
	 */
	public void setErrorClass(String errorClass) {
		this.errorClass = errorClass;
	}
	/**
	 * @return the errorMethod
	 */
	public String getErrorMethod() {
		return errorMethod;
	}
	/**
	 * @param errorMethod the errorMethod to set
	 */
	public void setErrorMethod(String errorMethod) {
		this.errorMethod = errorMethod;
	}
	/**
	 * @return the errorSummary
	 */
	public String getErrorSummary() {
		return errorSummary;
	}
	/**
	 * @param errorSummary the errorSummary to set
	 */
	public void setErrorSummary(String errorSummary) {
		this.errorSummary = errorSummary;
	}
	/**
	 * @return the errorDetail
	 */
	public String getErrorDetail() {
		return errorDetail;
	}
	/**
	 * @param errorDetail the errorDetail to set
	 */
	public void setErrorDetail(String errorDetail) {
		if(errorDetail != null && errorDetail.length() > GlobalConstants.ERROR_DETAIL_SIZE)
			this.errorDetail = errorDetail.substring(GlobalConstants.ERROR_INITIAL_INDEX, GlobalConstants.ERROR_DETAIL_SIZE);
		else
			this.errorDetail = errorDetail;		
	}	
}