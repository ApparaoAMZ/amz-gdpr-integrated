package com.amazon.gdpr.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This controller is the main page of the Heroku GDPR UI 
 * This will be called everytime the Heroku GDPR webapp is loaded
 ****************************************************************************************/
@Controller
public class WelcomePageController {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_WELCOMEPAGECONTROLLER;
	
	/**
	 * This method gets called when the main page of the Heroku GDPR UI is called 
	 * @return  
	 */
	@RequestMapping("/")
	public String welcome(Model model) {
		String CURRENT_METHOD = "welcome";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		return "welcome";
	}
}