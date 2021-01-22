package com.amazon.gdpr.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This controller loads the Current Input Details UI page 
 * This will be called everytime the user clicks on the Current Input Details link 
 ****************************************************************************************/
@Controller
public class InputDetailController {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_INPUTDETAILCONTROLLER;
		
	/**
	 * This method is invoked when Current Input Details link is clicked
	 * @param model
	 * @return The input details UI page
	 */
	@RequestMapping("/inputDetails")
	public String inputDetails(Model model) {
		String CURRENT_METHOD = "inputDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		/*InputDetailsVw view = dbAccessService.fetchInputDetails();
		model.addAttribute("users", view.userList);
		model.addAttribute("categories", view.categoryList);*/
		return "inputDetails";	
	}
}