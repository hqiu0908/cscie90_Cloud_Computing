package hu.cloud.edu;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;


public class MySimpleDB {

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
        AmazonSimpleDB sdb = new AmazonSimpleDBClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sdb.setRegion(usEast1);

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SimpleDB");
        System.out.println("===========================================\n");

        try {
            // Create a domain
            String myDomain = "Celebrities";
            System.out.println("Creating domain called " + myDomain + ".\n");
            sdb.createDomain(new CreateDomainRequest(myDomain));

            // List domains
            System.out.println("Listing all domains in your account:\n");
            for (String domainName : sdb.listDomains().getDomainNames()) {
                System.out.println("  " + domainName);
            }
            System.out.println();

            // Put data into a domain
            System.out.println("Putting data into " + myDomain + " domain.\n");
            sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomain, createSampleData()));

            // Select data from a domain
            // Notice the use of backticks around the domain name in our select expression.
            String selectExpression = "select * from `" + myDomain + "` where Movie = 'Roman Holiday'";
            System.out.println("Selecting: " + selectExpression + "\n");
            SelectRequest selectRequest = new SelectRequest(selectExpression);
            for (Item item : sdb.select(selectRequest).getItems()) {
                System.out.println("  Item");
                System.out.println("    Name: " + item.getName());
                for (Attribute attribute : item.getAttributes()) {
                    System.out.println("      Attribute");
                    System.out.println("        Name:  " + attribute.getName());
                    System.out.println("        Value: " + attribute.getValue());
                }
            }
            System.out.println();

            // Replace/Change the year of Nobel prize award of a Nobel laureate.
            System.out.println("Replacing YearOfNobel of Albert-Einstein with 1922.\n");
            List<ReplaceableAttribute> replaceableAttributes = new ArrayList<ReplaceableAttribute>();
            replaceableAttributes.add(new ReplaceableAttribute("YearOfNobel", "1922", true));
            sdb.putAttributes(new PutAttributesRequest(myDomain, "Albert-Einstein", replaceableAttributes));

            // Delete a movie star and all of its attributes
            System.out.println("Deleting star Jennifer Aniston.\n");
            sdb.deleteAttributes(new DeleteAttributesRequest(myDomain, "Jennifer-Aniston"));

            // Delete a domain
            // System.out.println("Deleting " + myDomain + " domain.\n");
            // sdb.deleteDomain(new DeleteDomainRequest(myDomain));
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    /* Creates an array of SimpleDB ReplaceableItems populated with sample data. */
    private static List<ReplaceableItem> createSampleData() {
        List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();

        sampleData.add(new ReplaceableItem("Angelina-Jolie").withAttributes(
                new ReplaceableAttribute("Name", "Angelina Jolie Pitt", true),
                new ReplaceableAttribute("Movie", "Lara Croft: Tomb Raider", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Angelina_Jolie.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Angelina_Jolie.docx", true)));

        sampleData.add(new ReplaceableItem("Audrey-Hepburn").withAttributes(
                new ReplaceableAttribute("Name", "Audrey Hepburn", true),
                new ReplaceableAttribute("Movie", "Roman Holiday", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Audrey_Hepburn.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Audrey_Hepburn.docx", true)));

        sampleData.add(new ReplaceableItem("Jennifer-Aniston").withAttributes(
                new ReplaceableAttribute("Name", "Jennifer Joanna Aniston", true),
                new ReplaceableAttribute("Movie", "The Good Girl", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/images/Img_Jennifer_Aniston.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/stars/resumes/Resume_Jennifer_Aniston.docx", true)));

        sampleData.add(new ReplaceableItem("Albert-Einstein").withAttributes(
                new ReplaceableAttribute("Name", "Albert Einstein", true),
                new ReplaceableAttribute("Movie", "Any Movie", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Albert_Einstein.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Albert_Einstein.docx", true),
                new ReplaceableAttribute("YearOfNobel", "1921", true),
                new ReplaceableAttribute("FieldOfScience", "Physics", true)));

        sampleData.add(new ReplaceableItem("Hermann-Muller").withAttributes(
                new ReplaceableAttribute("Name", "Hermann Joseph Muller", true),
                new ReplaceableAttribute("Movie", "Any Movie", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Hermann_Joseph_Muller.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Hermann_Joseph_Muller.docx", true),
                new ReplaceableAttribute("YearOfNobel", "1946", true),
                new ReplaceableAttribute("FieldOfScience", "Physiology or Medicine", true)));

        sampleData.add(new ReplaceableItem("Werner-Heisenberg").withAttributes(
                new ReplaceableAttribute("Name", "Werner Karl Heisenberg", true),
                new ReplaceableAttribute("Movie", "Any Movie", true),
                new ReplaceableAttribute("PictureURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/images/Img_Werner_Karl_Heisenberg.jpg", true),
                new ReplaceableAttribute("ResumeURL", "https://s3.amazonaws.com/hqiu-hu-cloud-computing-hw6/nobels/resumes/Resume_Werner_Karl_Heisenberg.docx", true),
                new ReplaceableAttribute("YearOfNobel", "1932", true),
                new ReplaceableAttribute("FieldOfScience", "Physics", true)));

        return sampleData;
    }
}
