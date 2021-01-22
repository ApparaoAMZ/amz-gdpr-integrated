package com.amazon.gdpr.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazon.gdpr.processor.TagDataProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service performs the Depersonalization activity on the Heroku Data 
 * This will be invoked by the GDPRController 
****************************************************************************************/
@Service
public class DepersonalizationService {
	public static String MODULE_DEPERSONALIZATION = GlobalConstants.MODULE_DEPERSONALIZATION;
	public static String STATUS_SUCCESS = GlobalConstants.STATUS_SUCCESS;	
	
	@Autowired
	TagDataProcessor tagDataProcessor;
	
	/**
	 * Based on the Summary detail captured, depersonalization is performed 
	 * @param runId The current runid to track the depersonalization
	 */
	public void depersonalize(long runId) {
		try { 
			tagDataProcessor.taggingInitialize(runId);
		} catch(GdprException exception) {
			
		}
	}
}