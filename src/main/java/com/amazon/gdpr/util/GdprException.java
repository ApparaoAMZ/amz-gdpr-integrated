package com.amazon.gdpr.util;

public class GdprException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	String exceptionMessage;
	String exceptionDetail;

	/**
	 * @param exceptionMessage
	 */
	public GdprException(String exceptionMessage, String exceptionDetail) {
		super();
		this.exceptionMessage = exceptionMessage;
		this.exceptionDetail = exceptionDetail;
	}
	/**
	 * @return the exceptionMessage
	 */
	public String getExceptionMessage() {
		return exceptionMessage;
	}

	/**
	 * @param exceptionMessage the exceptionMessage to set
	 */
	public void setExceptionMessage(String exceptionMessage) {
		this.exceptionMessage = exceptionMessage;
	}
	/**
	 * @return the exceptionDetail
	 */
	public String getExceptionDetail() {
		return exceptionDetail;
	}
	/**
	 * @param exceptionDetail the exceptionDetail to set
	 */
	public void setExceptionDetail(String exceptionDetail) {
		this.exceptionDetail = exceptionDetail;
	}
	
}