package edu.hu.as;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


/**
 * reads a message from each queue defined for user, waiting CONSUMER_INTERVAL_ms miliseconds
 * between each read
 *
 * you can specify consumer_interval_ms from command line:
 * $> java consumer <interval in ms>
 */
public class ServerSideConsumer {

	static final int CONSUMER_INTERVAL_ms = 5000;

	public static void main(String[] args) throws Exception {

		/* replace with your access key and secret key */
		AmazonSQS sqs = new AmazonSQSClient(new BasicAWSCredentials("Access key", "Secret access key"));

		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);

		int consumer_interval_ms = args.length > 0 ? Integer.valueOf(args[0]).intValue() : CONSUMER_INTERVAL_ms;

		System.out.println("===========================================");
		System.out.println("CONSUMER");
		System.out.println("===========================================\n");

		try {
			while (true) {
				// List queues
				for (String queueUrl : sqs.listQueues().getQueueUrls()) {
					System.out.println("\nQueueUrl: " + queueUrl);

					// Receive messages
					System.out.print("Checking msg: ");
					Message msg = consumeMsg(sqs, queueUrl);
					if (msg != null) {
						System.out.println(msg.getBody());
						String messageReceiptHandle = msg.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
					} else {
						System.out.println("No message consumed from queue.");
					}
				}
				Thread.sleep(consumer_interval_ms);
				System.out.println();
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

	private static Message consumeMsg(AmazonSQS qsvc, String queueUrl) {
		// Receive messages
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
				.withMaxNumberOfMessages(new Integer(1));
		List<Message> messages = qsvc.receiveMessage(receiveMessageRequest).getMessages();
		return messages.isEmpty() ? null : messages.get(0);
	}

}
