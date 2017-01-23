import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ListTopicsResult;
import java.util.List;

public class Publish {
	public static void main(String[] args)throws Exception {	
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. ", e);
		}
		AmazonSNS sns = new AmazonSNSClient(credentials);
		try {
			// Fetch topic ARN-a  
			// Publish a message to all topics
			ListTopicsResult listTopRes = sns.listTopics();
			List<Topic> topicList = listTopRes.getTopics();
			for (Topic topic : topicList) {          
				String myTopicArn = topic.getTopicArn();                  
				sns.publish(new PublishRequest()
						.withTopicArn(myTopicArn)
						.withMessage("This is my message to all topics.")
						.withSubject("Message sent to " + myTopicArn));
			}

		} catch (AmazonServiceException ase) {
		} catch (AmazonClientException ace) {
		}
	}
}
