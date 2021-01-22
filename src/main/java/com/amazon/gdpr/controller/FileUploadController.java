package com.amazon.gdpr.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.amazon.gdpr.processor.AnonymizationFileProcessor;
import com.amazon.gdpr.processor.GdprDataProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.AnonymizationInputView;

/****************************************************************************************
 * This controller loads the GDPR Anonymization File Upload UI page 
 * This will be called everytime the user clicks on the Upload Anonymization 
 ****************************************************************************************/
@Controller
public class FileUploadController {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_FILEUPLOADCONTROLLER;
	
	public String uploadFileStatus = "";
	
	@Autowired
	AnonymizationFileProcessor anonymizationFileProcessor;
	
	@Autowired
	GdprDataProcessor gdprDataProcessor;
	
	/**
	 * This method is invoked when Upload Anonymization link is clicked
	 * This initializes the objects and loads the fileUpload page 
	 * @param model 
	 * @return The fileUpload page 
	 */
	@GetMapping("/fileUpload")
	public String loadAnonymizationForm(Model model) {
		String CURRENT_METHOD = "loadAnonymizationForm";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		model.addAttribute(GlobalConstants.ATTRIBUTE_MESSAGE, GlobalConstants.MSG_FILE_UPLOAD);
		return GlobalConstants.TEMPLATE_FILEUPLOAD;
	}
	
	/**
	 * This method is invoked when the submit button in the fileUpload page is clicked
	 * The file is extracted, transformed and loaded into respective tables
	 * @param file The file uploaded in the fileUpload UI page
	 * @param model
	 * @return	The fileUpload page with the status
	 */
	@PostMapping("/processFile")
    public String uploadMultipartFile(@RequestParam("uploadfile") MultipartFile file, Model model) {
		String CURRENT_METHOD = "uploadMultipartFile";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");		
		String uploadFileStatus = "";
		
		if(file.isEmpty()){
			model.addAttribute(GlobalConstants.ATTRIBUTE_MESSAGE, GlobalConstants.ERR_FILE_EMPTY);
			return GlobalConstants.TEMPLATE_FILEUPLOAD;
		}
			
	    try {
	    	List<AnonymizationInputView> lstAnonymizationInputView = anonymizationFileProcessor.parseAnonymizationFile(file);
	    	if(lstAnonymizationInputView != null && lstAnonymizationInputView.size() > 0){
	    		uploadFileStatus = GlobalConstants.MSG_FILE_ROWS_PROCESSED+lstAnonymizationInputView.size();	    		
	    		int insertImpactFieldCount = gdprDataProcessor.loadInputFieldDetails(lstAnonymizationInputView);
	    		uploadFileStatus = uploadFileStatus + GlobalConstants.SEMICOLON_STRING + GlobalConstants.MSG_IMPACT_FIELD_ROWS + insertImpactFieldCount;	    		
	    		int insertAnonymizationDetailCount = anonymizationFileProcessor.loadAnonymizationDetails(lstAnonymizationInputView);
	    		uploadFileStatus = uploadFileStatus + GlobalConstants.SEMICOLON_STRING + GlobalConstants.MSG_ANONYMIZATION_DTL_ROWS + insertAnonymizationDetailCount;
	    	}else{
	    		uploadFileStatus = GlobalConstants.MSG_FILE_ROWS_PROCESSED_0;
	    	}
	    	model.addAttribute(GlobalConstants.ATTRIBUTE_MESSAGE, uploadFileStatus);
	    } catch(GdprException exception) {
			model.addAttribute(GlobalConstants.ATTRIBUTE_MESSAGE, exception.getExceptionMessage() + GlobalConstants.ERR_DISPLAY);
		} 
        return GlobalConstants.TEMPLATE_FILEUPLOAD;
    }
}