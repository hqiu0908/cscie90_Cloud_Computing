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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SNSActions;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.util.Topics;
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
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

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
public class SNSToSQS {

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
		AmazonSNS sns = new AmazonSNSClient(credentials);

		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon SNS and SQS");
		System.out.println("===========================================\n");

		try {
			// Create a SQS queue
			System.out.println("Creating a new SQS queue called HqiuSQSQueue.\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("HqiuSQSQueue");
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

			// Retrieve SQS Amazon Resource Name
			GetQueueAttributesRequest getQARequest = new GetQueueAttributesRequest().withQueueUrl(myQueueUrl);
			GetQueueAttributesResult getQAResult =  sqs.getQueueAttributes(getQARequest.withAttributeNames("QueueArn"));
			Map <String,String> attributeMap = getQAResult.getAttributes();
			String myQueueArn = attributeMap.get("QueueArn");
			System.out.println("Queue created: " + myQueueArn);

			// Create a SNS Topic           
			System.out.println("Creating HqiuSNSTopic.\n");
			CreateTopicRequest createTopicRequest = new CreateTopicRequest().withName("HqiuSNSTopic");

			// Retrieve SNS Amazon Resource Name
			String myTopicArn = sns.createTopic(createTopicRequest).getTopicArn();
			System.out.println("Topic created: " + myTopicArn);
			
			// Subscribe SQS queue to SNS topic on both sides
			// Method 1:
			Topics.subscribeQueue(sns, sqs, myTopicArn, myQueueUrl);
			
			// Method 2:
			// Send subscribe request
			// SubscribeRequest subReq = new SubscribeRequest(myTopicArn, "sqs", myQueueArn);
			// SubscribeResult subRes = sns.subscribe(subReq);
			
			System.out.println("\nSubscribed " + myQueueArn + " to topic: " + myTopicArn + "\n");
			
			// Set policy on topic to allow open subscriptions
			Policy snsPolicy = new Policy().withStatements(
					new Statement(Effect.Allow)
					.withPrincipals(Principal.AllUsers)
					.withActions(SNSActions.Subscribe));
			sns.setTopicAttributes(new SetTopicAttributesRequest(myTopicArn, "Policy", snsPolicy.toJson()));

			// Set the queue policy to allow SNS to publish messages
			Policy sqsPolicy = new Policy().withStatements(
					new Statement(Effect.Allow)
					.withPrincipals(Principal.AllUsers)
					.withResources(new Resource(myQueueArn))
					.withActions(SQSActions.SendMessage)
					.withConditions(ConditionFactory.newSourceArnCondition(myTopicArn)));

			HashMap<String, String> queueAttributes = new HashMap<String, String>();
			queueAttributes.put("Policy", sqsPolicy.toJson());
			sqs.setQueueAttributes(new SetQueueAttributesRequest(myQueueUrl,queueAttributes));

			// Public message from SNS Topic to SQS queue
			sns.publish(new PublishRequest()
					.withTopicArn(myTopicArn)
					.withMessage("This is my message to SQS queue.")
					.withSubject("Message sent to " + myTopicArn));
			
			// Receive messages from SQS queue
			List<Message> messages = receiveMessage(sqs, myQueueUrl);

			// Delete a queue
			// System.out.println("Deleting the test queue.\n");
			// sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));

			// Delete a Topic
			// System.out.println("\nDeleting the test Topic.\n");
			// sns.deleteTopic(new DeleteTopicRequest().withTopicArn(myTopicArn));			
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

		System.out.println("\nReceiving messages from MyQueue.\n");
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
