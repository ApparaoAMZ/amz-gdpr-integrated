package com.amazon.gdpr.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.input.AnonymizationDetail;
import com.amazon.gdpr.model.gdpr.input.Country;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;
import com.amazon.gdpr.view.AnonymizationInputView;

/****************************************************************************************
 * This Processor extracts, transforms and loads the Anonymization input tracker.  
 ****************************************************************************************/
@Component
public class AnonymizationFileProcessor {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZATIONPROCESSOR;
	public String anonymizationProcessStatus = "";
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
		
	/**
	 * This method navigates the list of AnonymizationInputView and inserts the RUN_ANONYMIZATION_MAPPING table
	 * @param runId
	 * @return
	 */
	public int loadRunAnonymization(long runId, List<String> selectedCountries) throws GdprException {
		String CURRENT_METHOD = "loadRunAnonymization";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		int insertRunAnonymizationCount = 0;
		
		RunErrorMgmt runErrorMgmt = null;
		Boolean exceptionOccured = false;
		Date moduleStartDateTime = null;
		Date moduleEndDateTime = null;
		List<Country> lstCountry = null;
		String errorDetails = "";
		
		try {
			moduleStartDateTime = new Date();
			lstCountry = gdprInputFetchDaoImpl.fetchCountry(selectedCountries);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: selectedCountries : "+selectedCountries.toString());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstCountry : "+lstCountry.toString());
		}catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_FETCH_COUNTRY_DETAIL);
			exceptionOccured = true;
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_FETCH_COUNTRY_DETAIL, exception.getMessage());			
		}
		
		try {
			if(! exceptionOccured) {
				if(lstCountry != null && lstCountry.size() > 0){
					for(Country country : lstCountry){
						insertRunAnonymizationCount = insertRunAnonymizationCount + 
								gdprOutputDaoImpl.batchInsertRunAnonymizeMapping(runId, country.getCountryCode(), country.getRegion());
					}
					anonymizationProcessStatus  = anonymizationProcessStatus + GlobalConstants.RUN_ANONYMIZATION_INSERT+insertRunAnonymizationCount;
				}
			}			
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD);
			exceptionOccured = true;
			exception.printStackTrace();
			runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD, exception.getMessage());
			errorDetails = errorDetails + exception.getMessage();
		}
		try {
			if (exceptionOccured) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				anonymizationProcessStatus  = anonymizationProcessStatus + GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD; 
				
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD  
					+ GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exceptionOccured = true;
			exception.printStackTrace();
			anonymizationProcessStatus  = anonymizationProcessStatus + GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD +
					GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT;
			errorDetails = errorDetails + exception.getMessage();
		}
		try {
			String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
			moduleEndDateTime = new Date();
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
					GlobalConstants.SUB_MODULE_ANONYMIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
					moduleEndDateTime, anonymizationProcessStatus, errorDetails);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			if(exceptionOccured)
				throw new GdprException(GlobalConstants.ERR_RUN_ANONYMIZATION_LOAD, errorDetails);
		} catch(GdprException exception) {
			exceptionOccured = true;
			errorDetails = errorDetails + exception.getMessage();
			anonymizationProcessStatus = anonymizationProcessStatus + GlobalConstants.ERR_RUN_MODULE_MGMT_INSERT;
			throw new GdprException(anonymizationProcessStatus, errorDetails);
		}
		
		return insertRunAnonymizationCount;
	}
	
	/**
	 * This method navigates the list of AnonymizationInputView to identify new AnonymizationDetail added and inserts it into table 
	 * @param lstAnonymizationInputView
	 */
	public int loadAnonymizationDetails(List<AnonymizationInputView> lstAnonymizationInputView) throws GdprException{
		String CURRENT_METHOD = "loadAnonymizationDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		Map<String, String> mapImpactTable = new HashMap<String,String>();
		Map<String, String> mapImpactField = new HashMap<String,String>();
		Map<String, String> mapCategory = new HashMap<String,String>();
		List<AnonymizationDetail> lstAnonymizationDetail = new ArrayList<AnonymizationDetail>();
		int insertCount=0;
				
		Set<AnonymizationDetail> setAnonymizationDetail = new HashSet<AnonymizationDetail>();
		List<AnonymizationDetail> lstAnonymizationDetailUpdated = new ArrayList<AnonymizationDetail>();
		RunErrorMgmt runErrorMgmt = null;
		String errorDetails = "";
		
		try {
			mapImpactTable = gdprInputDaoImpl.fetchImpactTableMap(GlobalConstants.IMPACTTABLE_MAP_NAMETOID);
			mapImpactField = gdprInputDaoImpl.fetchImpactFieldMap();
			mapCategory = gdprInputDaoImpl.fetchCategoryDetails();
			lstAnonymizationDetail = gdprInputDaoImpl.fetchAnonymizationDetails();
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_GDPR_INPUT_ALL_FETCH);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_GDPR_INPUT_ALL_FETCH, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_GDPR_INPUT_ALL_FETCH, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " +GlobalConstants.ERR_GDPR_INPUT_ALL_FETCH
					+ GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();			
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_GDPR_INPUT_ALL_FETCH + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		try {
			for(AnonymizationInputView anonymizationInputView : lstAnonymizationInputView){
				String strImpactTableId = mapImpactTable.get(anonymizationInputView.getObject()); 
				
				//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Table Name : "+anonymizationInputView.getObject()+" strImpactTableId : " + strImpactTableId);
				if (strImpactTableId == null || GlobalConstants.EMPTY_STRING.equalsIgnoreCase(strImpactTableId)) {
					continue;
				}
				
				String strImpactFieldId = mapImpactField.get(strImpactTableId+anonymizationInputView.getApiName());
				//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Field Name : "+anonymizationInputView.getApiName()+" strImpactFieldId : " + strImpactFieldId);
				if (strImpactFieldId == null || GlobalConstants.EMPTY_STRING.equalsIgnoreCase(strImpactFieldId)) {
					continue;
				}			
				
				int impactFieldId =  Integer.parseInt(strImpactFieldId);
				String strCategoryId = mapCategory.get(anonymizationInputView.categoryName);
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: categoryId : "+strCategoryId);
				if (strCategoryId == null || GlobalConstants.EMPTY_STRING.equalsIgnoreCase(strCategoryId)) {
					continue;
				}
				
				int categoryId = Integer.parseInt(strCategoryId);
				AnonymizationDetail anonymizationDetail = new AnonymizationDetail(impactFieldId, categoryId, anonymizationInputView.region, 
						anonymizationInputView.countryCode, anonymizationInputView.chosenTransformation, GlobalConstants.STATUS_ACTIVE);
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: anonymizationDetail : "+anonymizationDetail.toString());
				setAnonymizationDetail.add(anonymizationDetail);
				
			}
			if(setAnonymizationDetail != null && setAnonymizationDetail.size() > 0){
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: setAnonymizationDetail : "+setAnonymizationDetail.size());
				lstAnonymizationDetailUpdated.addAll(setAnonymizationDetail);
				Collections.sort(lstAnonymizationDetailUpdated, new SortByFieldId());
				if (lstAnonymizationDetail != null){
					//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizationDetail : "+lstAnonymizationDetail);
					Collections.sort(lstAnonymizationDetail, new SortByFieldId());
					boolean duplicateFlag = lstAnonymizationDetailUpdated.removeAll(lstAnonymizationDetail);
					//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: duplicateFlag : "+duplicateFlag);					
					//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizationDetailUpdated : "+lstAnonymizationDetailUpdated);
				}
					
				gdprInputDaoImpl.batchInsertAnonymizationDetail(lstAnonymizationDetailUpdated, SqlQueriesConstant.BATCH_ROW_COUNT);
				insertCount = lstAnonymizationDetailUpdated.size();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: insertCount : "+insertCount);
			}
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_ANONYMIZATION_DETAIL_INSERT);
			exception.printStackTrace();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					GlobalConstants.ERR_ANONYMIZATION_DETAIL_INSERT, exception.getMessage());
			errorDetails = exception.getMessage();
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(GlobalConstants.ERR_ANONYMIZATION_DETAIL_INSERT, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_ANONYMIZATION_DETAIL_INSERT 
								+ GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_ANONYMIZATION_DETAIL_INSERT + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return insertCount;
	} 	

	/**
	 * This method reads through the Excel Anonymization file uploaded through UI
	 * Converts each rows into a list AnonymizationInputView Object
	 * @param file Anonymization File uploaded
	 * @return List<AnonymizationInputView> List of rows in the file
	 */
	public List<AnonymizationInputView> parseAnonymizationFile(MultipartFile file) throws GdprException {
		String CURRENT_METHOD = "parseAnonymizationFile";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");				

		List<AnonymizationInputView> lstAnonymizationInputView = null;
		Workbook workbook;
		RunErrorMgmt runErrorMgmt = null;
		String errorMessge = "";
		String errorDetails = "";
		
		try {
    		workbook = new XSSFWorkbook(file.getInputStream());     
    		Sheet sheet = workbook.getSheet(GlobalConstants.FILE_SHEET_NAME);
    		Iterator<Row> rows = sheet.iterator();
    		
    		int rowNumber = 0;
    		lstAnonymizationInputView = new ArrayList<AnonymizationInputView>();
    		Set<AnonymizationInputView> setAnonymizationInputView = new HashSet<AnonymizationInputView>();
    		while (rows.hasNext()) {
    			Row currentRow = rows.next();
    				
    			if(rowNumber == 0) {// skip header
    				rowNumber++;
    				continue;
    			}
    			
    			Iterator<Cell> cellsInRow = currentRow.iterator();
    			int cellIndex = 0;
    			
    			AnonymizationInputView anonymizationInputView= new AnonymizationInputView();
    			while (cellsInRow.hasNext()) { 
    				//Cell currentCell = cellsInRow.next();
    				String cellValue = cellsInRow.next().getStringCellValue();
    				cellValue = (cellValue != null && cellValue.length() > 0 ) ? cellValue.toUpperCase().trim() : GlobalConstants.EMPTY_STRING;
    				
    				switch(Integer.valueOf(cellIndex)){
    					case 0 :
    						anonymizationInputView.setObject(cellValue);
    						break;
    					case 1 :
    						anonymizationInputView.setFieldLabel(cellValue);
    						break;
    					case 2 :
    						anonymizationInputView.setApiName(cellValue);
    						break;
    					case 3 :
    						if(cellValue.contains("TEXT"))
    						{
    							anonymizationInputView.setType("TEXT");
    						}
    						else if(cellValue.contains("COMBOBOX"))
    						{
    							anonymizationInputView.setType("TEXT");
    						}
    						else {
    							anonymizationInputView.setType(cellValue);
    						}
    						break;
    					case 4 :
    						anonymizationInputView.setCategoryName(cellValue);
    						break;
    					case 5 :
    						anonymizationInputView.setRecommendedTransformation(cellValue);
    						break;
    					case 6 :
    						anonymizationInputView.setChosenTransformation(cellValue);
    						break;
    					case 7 :
    						anonymizationInputView.setRegion(cellValue);
    						break;
    					case 8 :
    						anonymizationInputView.setCountryCode(cellValue);
    						break;
    				}
    				cellIndex++;
    				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: rowNumber : "+rowNumber+" cellIndex : "+cellIndex+" cellValue "+cellValue);    				
    			}
    			setAnonymizationInputView.add(anonymizationInputView);
    		}
    		if(setAnonymizationInputView != null && setAnonymizationInputView.size() > 0)
    			lstAnonymizationInputView.addAll(setAnonymizationInputView);
    		workbook.close();
		} catch (IOException exception) {
			errorMessge= GlobalConstants.ERR_PARSE_ANONYMIZATION_IO;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+errorMessge);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					errorMessge, exception.getMessage());
		} catch(Exception exception) {
			errorMessge= GlobalConstants.ERR_PARSE_ANONYMIZATION;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+errorMessge);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					errorMessge, exception.getMessage());
		}
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(errorMessge, errorDetails);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + errorMessge+ GlobalConstants.ERR_IMPACT_FIELD_INSERT);
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(errorMessge + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT, errorDetails);
		}
		return lstAnonymizationInputView;
	}
	
	class SortByFieldId implements Comparator<AnonymizationDetail> { 
		@Override
	    public int compare(AnonymizationDetail a, AnonymizationDetail b)  {
			return a.getImpactFieldId() - b.getImpactFieldId();	    	  
	    } 
	}
}