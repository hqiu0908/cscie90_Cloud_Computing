/*
 * Copyright 2010-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (/Users/hqiu/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class SQSTest {

	public static void main(String[] args) throws Exception {

		/*
		 * The ProfileCredentialsProvider will return your [default]
		 * credential profile by reading from the credentials file located at
		 * (/Users/hqiu/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/hqiu/.aws/credentials), and is in valid format.",
							e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon SQS");
		System.out.println("===========================================\n");

		try {
			// Create a queue
			System.out.println("Creating a new SQS queue called MyQueue.\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");

			// Set the visibility timeout to 20 seconds
			createQueueRequest.addAttributesEntry("VisibilityTimeout", "20");

			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

			// List queues
			System.out.println("Listing all queues in your account.\n");
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				System.out.println("  QueueUrl: " + queueUrl);
			}
			System.out.println();

			// Send a message
			System.out.println("Sending the first message to MyQueue.\n");
			sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my first message text."));

			// Receive messages
			List<Message> messages = receiveMessage(sqs, myQueueUrl);

			// Receive the messages again
			messages = receiveMessage(sqs, myQueueUrl);

			// Wait for 30 seconds until the message reappears in the queue
			Thread.sleep(30000L);

			// Delete the first message
			messages = receiveMessage(sqs, myQueueUrl);
			System.out.println("Deleting a message.\n");
			String messageRecieptHandle = messages.get(0).getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));

			// Send another message
			System.out.println("Sending the second message to MyQueue.\n");
			sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my second message text."));            

			// Retrieve the second message
			messages = receiveMessage(sqs, myQueueUrl);

			// Retrieve the second message again
			messages = receiveMessage(sqs, myQueueUrl);

			// Wait for 30 seconds, receive the messages again
			Thread.sleep(30000L);
			messages = receiveMessage(sqs, myQueueUrl);

			// Send five more messages to the queue
			System.out.println("Sending five messages to MyQueue.\n");
			for (int i = 1; i <= 5; i++) {               
				String messageStr = "This is my " + i + "/5 message text.";
				sqs.sendMessage(new SendMessageRequest(myQueueUrl, messageStr));
			}

			// Receive all the messages
			// messages = receiveMessage(sqs, myQueueUrl);
			// Thread.sleep(30000L);

			// Retrieve all attributes of the queue.
			GetQueueAttributesRequest getQARequest = new GetQueueAttributesRequest().withQueueUrl(myQueueUrl);
			GetQueueAttributesResult getQAResult =  sqs.getQueueAttributes(getQARequest.withAttributeNames("All"));
			Map <String,String> attributeMap = getQAResult.getAttributes();

			// Display all attributes, especially the average number of messages in the queue.
			System.out.println();
			System.out.println("Number of messages in queue " + myQueueUrl + " is " +
					attributeMap.get("ApproximateNumberOfMessages"));

			System.out.println("Number of invisible messages in queue is " + 
					attributeMap.get("ApproximateNumberOfMessagesNotVisible"));

			System.out.println("\nDisplay all the attributes:");
			for (String key : attributeMap.keySet()) {
				System.out.println("  " + key + " is: " + attributeMap.get(key));            	
			}

			// Delete a queue
			System.out.println("\nDeleting the test queue.\n");
			sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
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

	private static List<Message> receiveMessage(AmazonSQS sqs, String myQueueUrl) {

		System.out.println("Receiving messages from MyQueue.\n");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

		for (Message message : messages) {
			System.out.println("  Message");
			System.out.println("    MessageId:     " + message.getMessageId());
			System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
			System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
			System.out.println("    Body:          " + message.getBody());
			for (Entry<String, String> entry : message.getAttributes().entrySet()) {
				System.out.println("  Attribute");
				System.out.println("    Name:  " + entry.getKey());
				System.out.println("    Value: " + entry.getValue());
			}
		}
		System.out.println();   

		return messages;
	}
}
