import java.util.HashMap;
import java.util.Map;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Topic;

public class CreateTopic {

	public static void main(String[] args) throws Exception {
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

		AmazonSNS sns = new AmazonSNSClient(credentials);
		try {
			// Create a Topic
			System.out.println("Creating MyTopicHQ.\n");
			CreateTopicRequest createTopicRequest = new CreateTopicRequest().withName("MyTopicHQ");

			// Retrieve Amazon Resource Name
			String myTopicArn = sns.createTopic(createTopicRequest).getTopicArn();
			System.out.println("Topic created: " + myTopicArn);

			Thread.sleep(1000);
			// List Topics
			System.out.println("List topics: ");
			for (Topic topic : sns.listTopics().getTopics()) {
				System.out.println(" TopicArn: " + topic.getTopicArn());
			}

			// Set Topic Name for subscribing my phone
			String topicName = "TopicHQ";
			System.out.println("\nSet topic name: " + topicName + "\n");
			SetTopicName(sns, myTopicArn, topicName);

			// Subscribe my email and phone to MyTopicHQ
			subscribeEmail(sns, myTopicArn, "hanjiaoqiu@g.harvard.edu");
			subscribeEmail(sns, myTopicArn, "qiuhanjiao@gmail.com");
			subscribePhone(sns, myTopicArn, "1-617-955-7630");

			// Fetch Topic attributes
			System.out.println("\nTopic attributes: ");
			fetchAttr(sns, myTopicArn);

			// Delete a Topic
			// System.out.println("\nDeleting the test Topic.\n");
			// sns.deleteTopic(new DeleteTopicRequest().withTopicArn(myTopicArn));
		} catch (AmazonServiceException ase) {
			System.out.println("Error Message: " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code: " + ase.getErrorCode());
			System.out.println("Error Type: " + ase.getErrorType());
			System.out.println("Request ID: " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void subscribeEmail(AmazonSNS sns, String myTopicArn, String email) {
		SubscribeRequest subReq = new SubscribeRequest(myTopicArn, "email", email);
		SubscribeResult subRes = sns.subscribe(subReq);
		String subscribedTopicArn = subRes.getSubscriptionArn();
		System.out.println("Subscribed " + email + " to topic: " + subscribedTopicArn);
	}

	private static void subscribePhone(AmazonSNS sns, String myTopicArn, String phone) {
		SubscribeRequest subReq = new SubscribeRequest(myTopicArn, "sms", phone);
		SubscribeResult subRes = sns.subscribe(subReq);
		String subscribedTopicArn = subRes.getSubscriptionArn();
		System.out.println("Subscribed " + phone + " to topic: " + subscribedTopicArn);
	}

	private static void fetchAttr(AmazonSNS sns, String myTopicArn) {
		GetTopicAttributesRequest getTARequest = new GetTopicAttributesRequest().withTopicArn(myTopicArn);
		GetTopicAttributesResult getTAResult = sns.getTopicAttributes(getTARequest);
		Map<String, String> attributes = new HashMap<String, String>();
		attributes = getTAResult.getAttributes();
		for (String key : attributes.keySet()) {
			System.out.println(key + ": " + attributes.get(key));
		}
	}

	private static void SetTopicName(AmazonSNS sns, String myTopicArn, String topicName) {
		SetTopicAttributesRequest setTARequest = new SetTopicAttributesRequest().withTopicArn(myTopicArn);
		setTARequest.withAttributeName("DisplayName").setAttributeValue(topicName);		
		sns.setTopicAttributes(setTARequest);
	}
}
