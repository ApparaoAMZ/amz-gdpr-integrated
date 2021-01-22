$( document ).ready(function() {
	
	// SUBMIT FORM
    $("#gdprform").submit(function(event) {
		// Prevent the form from submitting via the browser.
		event.preventDefault();
		$("#gdprsubmit").prop("disabled", true);
		$("#gdprrunstatus").html("Anonymization InProgress");
		ajaxPost();
	});
    
    
    function ajaxPost(){
    	
    	// PREPARE FORM DATA
    	var formData = {
    		runname : $("#runname").val()
    			}
    	//alert(formData.runname);
    	// DO POST
    	$.ajax({
			type : "POST",
			contentType : "application/json",
			url : "/gdprSubmit",
			data : JSON.stringify(formData),
			dataType : 'json',
			success : function(result) {
			//alert(result.status);
				if(result.status == "Ok"){
					$("#gdprrunstatus").html(result.data.runStatus);
					
				}else{
					$("#gdprrunstatus").html(result.data.runStatus);
				}
				console.log(result);
				 $("#gdprsubmit").prop("disabled", false);
			},
			error : function(e) {
				alert("Error!")
				console.log("ERROR: ", e);
				 $("#gdprsubmit").prop("disabled", false);
			}
		});
    	
    	// Reset FormData after Posting
    	resetData();

    }
    
    function resetData(){
    	$("#runname").val("");
    	
    }
})