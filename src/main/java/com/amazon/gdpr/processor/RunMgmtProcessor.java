package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.input.Country;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.GdprInput;

/****************************************************************************************
 * This processor verifies the previous failure run / initiates a current run 
 * Any processing or updates related to the RunMgmt tables are performed here
 ****************************************************************************************/
@Component
public class RunMgmtProcessor {
		
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_RUNMGMTPROCESSOR;
	private static String STATUS_FAILURE			= GlobalConstants.STATUS_FAILURE;
	public String initializeRunStatus = "";
	RunErrorMgmt runErrorMgmt=null;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
			
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	/**
	 * This method handled the initialization of the current run
	 * Takes a call whether the old failure run has to be proceeded on or a new run should be instantiated	 
	 * @param runName The description of the current run is being maintained
	 * @return The RunId is returned back to controller and this is passed on to all methods 
	 */
	public long initializeRun(String runName) throws GdprException {
		String CURRENT_METHOD = "initializeRun";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		long runId = 0;
		Boolean exceptionOccured = false;
		Date moduleStartDateTime = null;
		Date moduleEndDateTime = null;
		
		try{
			moduleStartDateTime = new Date();
			//Fetch lastRunId if failed
			RunModuleMgmt module = oldRunVerification();
			if(module == null){
				runId = initiateNewRun(runName);
				initializeRunStatus = GlobalConstants.MSG_NEW_RUN_INITIATED + runId;
			} else {
				runId = module.getRunId();
				initializeRunStatus = GlobalConstants.MSG_OLD_RUN_FETCHED + runId;
			}			
		} catch(GdprException exception) {
			initializeRunStatus = initializeRunStatus + exception.getExceptionMessage();
		}
		try {			
			String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
			moduleEndDateTime = new Date();
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, GlobalConstants.SUB_MODULE_RUN_INITIALIZE,
					moduleStatus, moduleStartDateTime, moduleEndDateTime, initializeRunStatus);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
		} catch(GdprException exception) {
			exceptionOccured = true;
			initializeRunStatus = initializeRunStatus + exception.getExceptionMessage();
		}		
		return runId;
	}		
	
	/**
	 * The last failure run is fetched and verified if it needs to be proceeded on 
	 * @return Returns the last Failure entry of the RunModuleMgmt
	 */
	public RunModuleMgmt oldRunVerification() throws GdprException {
		String CURRENT_METHOD = "oldRunVerification";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try{
			RunMgmt runMgmt = runMgmtDaoImpl.fetchLastRunDetail();
			if(runMgmt != null && STATUS_FAILURE.equalsIgnoreCase(runMgmt.getRunStatus()) ){
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Past Failure RunID : "+runMgmt.getRunId());
			}else {
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: No past failure run available. ");
				return null;
			}
		} catch(Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_OLD_RUN_FETCH);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_OLD_RUN_FETCH, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_OLD_RUN_FETCH, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_OLD_RUN_FETCH + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_OLD_RUN_FETCH + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return null;
	}
		
	/**
	 * A new run is initiated in this method. An entry is made in the RunMgmt table for this run
	 * @param runName The description of the current run is being maintained
	 * @return The RunId is returned back to controller and this is passed on to all methods
	 */
	public long initiateNewRun(String runName) throws GdprException {
		String CURRENT_METHOD = "initiateNewRun";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: New run initiation in progress.");
		long runId = 0;		
		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try {
			runMgmtDaoImpl.initiateNewRun(runName);
			runId = runMgmtDaoImpl.fetchLastRunDetail().getRunId(); 
		} catch(Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_NEW_RUN_INITIATION);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_NEW_RUN_INITIATION, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_NEW_RUN_INITIATION, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_NEW_RUN_INITIATION + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_NEW_RUN_INITIATION + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return runId;
	}
	
	public GdprInput loadCountryDetail() throws GdprException {
		String CURRENT_METHOD = "loadCountryDetail";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Loading the GdprInput Object. ");
		
		GdprInput gdprInput = new GdprInput();
		Map<String, List<String>> mapRegionCountry = new HashMap<String, List<String>>();
		List<String> lstRegion = new ArrayList<String>();
		List<String> lstCountryCode = new ArrayList<String>();
		
		String loadCountryDtlStatus = "";
		String errorDetails = "";
				
		try {
			List<Country> lstCountry = gdprInputFetchDaoImpl.fetchAllCountries(); 
			
			String prevRegion = null;
			List<String> lstRegionsCountry = new ArrayList<String>();
			Set<String> setRegion = new HashSet<String>();
			
			for(Country country : lstCountry) {
				String currentRegion = country.getRegion();
				String currentCountry = country.getCountryCode();
				
				if(prevRegion != null && !(prevRegion.equalsIgnoreCase(currentRegion))){
					mapRegionCountry.put(prevRegion, lstRegionsCountry);
					lstRegionsCountry = new ArrayList<String>();				
				}
				lstRegionsCountry.add(currentCountry);
				lstCountryCode.add(currentCountry);
				setRegion.add(currentRegion);
				prevRegion = currentRegion;
			}
			mapRegionCountry.put(prevRegion, lstRegionsCountry);
			lstRegion.addAll(setRegion);
			
			gdprInput.setLstRegion(lstRegion);
			gdprInput.setLstCountry(lstCountryCode);
			gdprInput.setMapRegionCountry(mapRegionCountry);
			loadCountryDtlStatus = GlobalConstants.MSG_LOAD_FORM;
		} catch(Exception exception){			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_LOAD_FORM);
			exception.printStackTrace();
			loadCountryDtlStatus = GlobalConstants.ERR_LOAD_FORM;
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_LOAD_FORM, exception.getMessage());			
		}
		
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(loadCountryDtlStatus, errorDetails);
			}				
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_LOAD_FORM  
					+ GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			loadCountryDtlStatus = loadCountryDtlStatus + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT;
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(loadCountryDtlStatus, errorDetails);
		}
		return gdprInput;
	}
}