package com.amazon.gdpr.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This controller loads the Salesforce Details UI page 
 * This will be called everytime the user clicks on the Salesforce Details link 
 ****************************************************************************************/
@Controller
public class SalesforceDetailController {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_SALESFORCEDETAILCONTROLLER;
	
	/**
	 * This method is invoked when Salesforce Details link is clicked
	 * @param model
	 * @return The Salesforce details UI page
	 */
	@RequestMapping("/salesforceDetails")
	public String salesforceDetails(Model model) {
		String CURRENT_METHOD = "salesforceDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		/*SalesforceDetailsVw view = HerokuConnectService.fetchSalesforceDetails();
		model.addAttribute("company", view.countryMappingList);*/
		return "salesforceDetails";	
	}
}
