package com.amazon.gdpr.response;

public class Response {
    public static final String NOT_FOUND = "Not found";
    public static final String OK = "Ok";
	private String status;
	private Object data;
	
	public Response(){
		
	}
	
	public Response(String status, Object data){
		this.status = status;
		this.data = data;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}
}
