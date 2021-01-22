package com.amazon.gdpr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This is the Spring Boot Servlet Initializer class for GDPR Application web
 * This will be initiated during application deployment in Heroku
 ****************************************************************************************/
@SpringBootApplication
public class GdprApplication extends SpringBootServletInitializer {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRAPPLICATION;
	
	/* (non-Javadoc)
	 * @see org.springframework.boot.web.support.SpringBootServletInitializer#configure(org.springframework.boot.builder.SpringApplicationBuilder)
	 */
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		String CURRENT_METHOD = "configure";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		setRegisterErrorPageFilter(false);
		return application.sources(GdprApplication.class);
	}
}