package com.amazon.gdpr.controller;

import java.time.LocalTime;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;

import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.service.BackupService;
import com.amazon.gdpr.service.DepersonalizationService;
import com.amazon.gdpr.service.InitService;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.GdprInput;

/****************************************************************************************
 * This controller in the main initiator. 
 * This will be invoked by the Heroku UI or the scheduler
 ****************************************************************************************/
@Controller
public class GdprProcessController {

	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRPROCESSCONTROLLER;
		
	long runId = 0;
	Map<String, RunSummaryMgmt> mapRunSummaryMgmt = null;
	
	@Autowired
	BackupService backupService;
	
	@Autowired
	InitService initService;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl; 	
	
	@Autowired
	DepersonalizationService depersonalizationService;
	/**
	 * This method is invoked when GDPR Initiate link is clicked
	 * This initializes the objects and loads the gdprInitiate page
	 * @param model
	 * @return The gdprInput page
	 */
	@GetMapping("/gdprInput")
	public String gdprInputForm(Model model) {
		String CURRENT_METHOD = "gdprInputForm";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		GdprInput gdprInputData = initService.loadGdprForm();
		GdprInput gdprInput = new GdprInput();
		if(gdprInputData != null) {
			gdprInput.setLstCountry(gdprInputData.getLstCountry());
		}		
		model.addAttribute("gdprInput", gdprInput);
		/* Verify the current status of the run from DB and display it */
		return GlobalConstants.TEMPLATE_GDPRINPUT;		
	}
	
	/**
	 * This method is invoked when Submit button in the GDPR Depersonalization Input Page is clicked
	 * This initiates the Depersonalization activity
	 * @param runName - Input to maintain the run information
	 * @return String - The gdprInput page
	 */
  	@PostMapping("/gdprSubmit")
	public String herokuDepersonalization(@ModelAttribute("gdprInput") GdprInput gdprInput, Model model) {
		String CURRENT_METHOD = "herokuDepersonalization";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		String runName = gdprInput.getRunName();
		// Init Service initiated
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Run Name : "+runName);
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Countries : "+gdprInput.getCountries());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Countries : "+gdprInput.getLstCountry().toString());
			
		String initServiceStatus =  initService.initService(runName, gdprInput.getLstCountry());	
		
		GdprInput gdprInputSubmitted = initService.loadGdprForm();
		
		long runId = runMgmtDaoImpl.fetchLastRunDetail().getRunId();

		System.out.println("runId-Run Name : " + runId);

		//String backupServiceStatus = backupService.backupService(runId);		
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: After Backup Service : "+backupServiceStatus+"::" + LocalTime.now());
		
		gdprInputSubmitted.setRunName(runName);
		gdprInputSubmitted.setRunStatus(initServiceStatus);
		gdprInputSubmitted.setLstSelectedCountry(gdprInput.getLstCountry());				
		
		model.addAttribute(GlobalConstants.ATTRIBUTE_GDPRINPUT, gdprInputSubmitted);
		return GlobalConstants.TEMPLATE_GDPRINPUT;
	}
				
	/**
	 * @param runId
	 * @param runSummaryMgmtMap
	 * @return
	 */
	public Map<String, RunSummaryMgmt> backupInitialize(long runId, Map<String, RunSummaryMgmt> runSummaryMgmtMap) {
		String CURRENT_METHOD = "backupInitialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				
		//runSummaryMgmtMap = backpupService.backupService(runId, runSummaryMgmtMap);
		return null;
	}	
}