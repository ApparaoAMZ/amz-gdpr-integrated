package com.amazon.gdpr.processor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.gdpr.output.DataLoad;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This processor Fetches and loads the data from the Data_Load Table
 ****************************************************************************************/
@Component
public class DataLoadProcessor {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_DATALOAD_PROCESSOR;
		
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	/**
	 * After the load of each table the Data_Load Table is loaded
	 * @param dataLoad The details of the DataLoad are passed on as input
	 */
	public void initiateDataLoad(long runId, DataLoad dataLoad) throws GdprException {
		String CURRENT_METHOD = "initiateDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Data Load update in progress.");
		RunErrorMgmt runErrorMgmt = null;
		String dataLoadStatus = "";
		String errorDetails = "";
		
		try {
			gdprOutputDaoImpl.insertDataLod(dataLoad);
		} catch(Exception exception) {
			dataLoadStatus = GlobalConstants.ERR_REORGANIZEINPUT_DATA_LOAD;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception occured");
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD, dataLoadStatus, exception.getMessage());
		}
		
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(dataLoadStatus, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + dataLoadStatus + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(dataLoadStatus + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
	}
}