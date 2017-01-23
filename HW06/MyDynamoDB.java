
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class MyDynamoDB {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/Users/hqiu/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;
    static String tableName;
    static Table table;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
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
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        dynamoDB.setRegion(usEast1);
    }

    public static void main(String[] args) throws Exception {
        init();
        
		System.out.println("Before table creation: ");
        listTables();

        try {
            tableName = "CELEBRITIES_SDK";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("\n\nTable " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("CelebrityName").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("CelebrityName").withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.awaitTableToBecomeActive(dynamoDB, tableName);
            }
            
            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);
            
    		System.out.println("\n\nAfter table creation: ");
            listTables();
            System.out.println();
            
            // Get the table
            table = new DynamoDB(dynamoDB).getTable(tableName);
    		
            // Add items
            addStar("Angelina-Jolie", "Angelina Jolie Pitt", "Lara Croft: Tomb Raider", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Angelina_Jolie.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Angelina_Jolie.docx");

            addStar("Audrey-Hepburn", "Audrey Hepburn", "Roman Holiday", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Audrey_Hepburn.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Audrey_Hepburn.docx");
            
            addStar("Jennifer-Aniston", "Jennifer Joanna Aniston", "The Good Girl", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Jennifer_Aniston.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Jennifer_Aniston.docx");
 
            addNobel("Albert-Einstein", "Albert Einstein", "Any Movie", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Albert_Einstein.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Albert_Einstein.docx",
            		1921, "Physics");
            
            addNobel("Hermann-Muller", "Hermann Joseph Muller", "Any Movie", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Hermann_Joseph_Muller.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Hermann_Joseph_Muller.docx",
            		1946, "Physiology or Medicine");
            
            addNobel("Werner-Heisenberg", "Werner Karl Heisenberg", "Any Movie", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Werner_Karl_Heisenberg.jpg", 
            		"https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Werner_Karl_Heisenberg.docx",
            		1932, "Physics");

            // Query items for Nobel laureates with a primary key Albert-Einstein from CELEBRITIES_SDK
            QuerySpec spec = new QuerySpec()
            		.withHashKey("CelebrityName", "Albert-Einstein")
            		.withFilterExpression("YearOfNobel between :v_start_yn and :v_end_yn")
            		.withValueMap(new ValueMap()
            				.withInt(":v_start_yn", 1920)
            				.withInt(":v_end_yn", 1922));
            ItemCollection<QueryOutcome> items = table.query(spec);
            
            System.out.println("\nQuery items for Nobel laureates with a primary key Albert-Einstein from table "+ tableName + ":");
            for (Item item: items) {
            	System.out.println(item);
            }
            
            // Scan items for Nobel laureates with a year attribute smaller than 1940 from CELEBRITIES_SDK
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.LT)
                .withAttributeValueList(new AttributeValue().withN("1940"));
            scanFilter.put("YearOfNobel", condition);
            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("\nScan items for Nobel laureates with a YearOfNobel attribute smaller than 1940 from table "+ tableName + ":");
            System.out.println("Result: " + scanResult);
                    
            // Scan items for Stars with a specific movie name from CELEBRITIES_CONSOLE.
            scanFilter = new HashMap<String, Condition>();
            condition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ.toString())
                .withAttributeValueList(new AttributeValue().withS("Roman Holiday"));
            scanFilter.put("Movie", condition);
            scanRequest = new ScanRequest("CELEBRITIES_CONSOLE").withScanFilter(scanFilter);
            scanResult = dynamoDB.scan(scanRequest);
            System.out.println("\nScan items for Stars with a movie named \"Roman Holiday\" from table CELEBRITIES_CONSOLE:");
            System.out.println("Result: " + scanResult);
            
            // Update the award year of one of the Nobles
            updateYearOfNobel("Albert-Einstein", 1920);
            
            // Delete the table
            deleteTable(tableName);
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

	private static void listTables() {
		// List all the tables
		TableCollection<ListTablesResult> tables = new DynamoDB(dynamoDB).listTables();
		Iterator<Table> iterator = tables.iterator();
		
		System.out.println("My DynamoDB tables:");
		while (iterator.hasNext()) {
			Table table = iterator.next();
			System.out.println("\t" + table.getTableName());
		}
	}
	
	private static void addStar(String keyName, String fullName, String movie, String picURL, String resumeURL) {

		System.out.println("Adding stars: " + keyName + "/" + fullName + "/" + movie + 
				"/" + picURL + "/" + resumeURL);
	
		/*		
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("CelebrityName", new AttributeValue(keyName));
        item.put("Name", new AttributeValue(fullName));
        item.put("Movie", new AttributeValue(movie));   
        item.put("PictureURL", new AttributeValue(picURL));
        item.put("ResumeURL", new AttributeValue(resumeURL));               
        
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		dynamoDB.putItem(putItemRequest);			
		*/
		
		Item item = new Item().withPrimaryKey("CelebrityName", keyName)
				.withString("Name", fullName)
				.withString("Movie", movie)
				.withString("PictureURL", picURL)
				.withString("ResumeURL", resumeURL);
		table.putItem(item);
	}
	
	private static void addNobel(String keyName, String fullName, String movie, String picURL, String resumeURL,
			Integer year, String field) {
	
		System.out.println("Adding nobel laureates: " + keyName + "/" + fullName + "/" + movie + 
				"/" + picURL + "/" + resumeURL + "/" + year + "/" + field);
		
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("CelebrityName", new AttributeValue(keyName));
        item.put("Name", new AttributeValue(fullName));
        item.put("Movie", new AttributeValue(movie));   
        item.put("PictureURL", new AttributeValue(picURL));
        item.put("ResumeURL", new AttributeValue(resumeURL));               
        item.put("YearOfNobel", new AttributeValue().withN(Integer.toString(year)));
        item.put("FieldOfScience", new AttributeValue(field));
        
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		dynamoDB.putItem(putItemRequest);
		
		// PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		// System.out.println("Result: " + putItemResult.toString());
	}
	
	public static void updateYearOfNobel(String keyName, int year) {
		System.out.println("\nUpdate the celebrity's attribute \"YearOfNobel\" to " + year +":");

	    System.out.println("Before update, the \"YearOfNobel\" is " + getYearOfNobel(keyName) + ".");
	    
		table.updateItem("CelebrityName", keyName,
					new AttributeUpdate("YearOfNobel").put(year));
	          	
	    System.out.println("The newly updated \"YearOfNobel\" is " + getYearOfNobel(keyName) + ".");
	}
	
	public static int getYearOfNobel(String keyName) {
		GetItemOutcome outcome = table.getItemOutcome(new GetItemSpec()
	               .withPrimaryKey("CelebrityName", keyName)
	               .withConsistentRead(true));
		
		Item item = outcome.getItem();
	    return item.getInt("YearOfNobel");	
	}
	
	private static void deleteTable(String tableName) {
		Table table = new DynamoDB(dynamoDB).getTable(tableName);
		
		if (table == null) 
			return;		
		table.delete();
		
		try {
			table.waitForDelete();
		} catch (InterruptedException e) {
			System.out.println("Failed to delete table: " + tableName);
			e.printStackTrace();
		}
		System.out.println("Table is deleted: " + tableName);
	}
}