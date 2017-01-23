import java.util.List;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.ConfirmSubscriptionResult;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;

public class OptIn {
	public static void main(String[] args) throws Exception {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials", e);
		}
		AmazonSNS sns = new AmazonSNSClient(credentials);

		System.out.println("===========================================");
		System.out.println("Accepting subscription, Opting-In");
		System.out.println("===========================================\n");
		try {
			String topicArn = "arn:aws:sns:us-east-1:217134905396:MyTopicHQ";
			// Endpoint=qiuhanjiao@gmail.com
			String token = "2336412f37fb687f5d51e6e241d7700bdeeb300d6040051d539adc048f86c09b33fe3d34f6ce360db1f7390c59c4481fe1db4cde12e0d69be4eee478a0627561b6b3e811f7ce184b7df4f64717486864bc00ea0d25131821faa94b4e6136395d973247dac095f1d6b59e2cf16957f374";               
	
			// Endpoint=hanjiaoqiu@g.harvard.edu
			// String token = "2336412f37fb687f5d51e6e241d7700bdeeb300d6040041e70e47fd455e1b00a6bf96ce9758e95ecfa10f1a59287405fc8558803cdcd5227fe83d43091e7a8b66d0568c0f0495e07fff3f973f08e5d1fa854b4d66347a981b651ce33f4e976d4aac03cd611f5bcde919ff6db3e56dd71";
			ConfirmSubscriptionResult conSubRes = sns.confirmSubscription(
					new ConfirmSubscriptionRequest(topicArn, token));    
			String subscribedTopicArn = conSubRes.getSubscriptionArn(); 
			
			// List Topic subscriptions
			ListSubscriptionsResult listSubResult = sns.listSubscriptions();
			List<Subscription> subscriptions = listSubResult.getSubscriptions();          
			for (Subscription sub : subscriptions) {
				System.out.println(sub.getEndpoint() + " " + sub.getOwner() + 
						" " + sub.getTopicArn());
			}
		}
		catch(AmazonServiceException ase) { }
		catch(AmazonClientException ace) { }
	}
}
