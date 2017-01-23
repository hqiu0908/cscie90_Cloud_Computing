package edu.hu.lambda;

import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class Hello {    
	public String myHandler(String myString, Context context) { 
		LambdaLogger logger = context.getLogger(); 
		logger.log("received : " + myString);
		return (myString + myString);
	}
}
