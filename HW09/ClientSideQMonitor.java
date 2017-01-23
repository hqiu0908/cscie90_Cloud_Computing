package edu.hu.client;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;

/**
 * prints current time and estimated length of queue (visible messages)
 * every WAIT_ms miliseconds, ad infinitum.
 */
public class ClientSideQMonitor {
	static final int WAIT_ms = 5000;
	public static void main(String[] args) throws Exception {
		AWSCredentials credentials = null;
		try { 
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"location (/Users/hqiu/.aws/credentials), and is in valid format." , e);         
		}
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		int wait_ms = args.length > 0 ? Integer.valueOf(args[0]).intValue() : WAIT_ms; 	
		System.out.println("Monitoring SQS");
		try {
			// get queue length
			GetQueueAttributesRequest attrReq = new GetQueueAttributesRequest( 
					"https://sqs.us-east-1.amazonaws.com/217134905396/MyQueue" );
			attrReq.setAttributeNames(Arrays.asList(  
					"ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"
					));            
			while (true) {
				GetQueueAttributesResult attrResult = sqs.getQueueAttributes(attrReq);
				Map<String,String> attrs = attrResult.getAttributes();
				System.out.println(
						(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date()))
						+ ": " + attrs.get("ApproximateNumberOfMessages"));
				Thread.sleep(wait_ms);
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
}
