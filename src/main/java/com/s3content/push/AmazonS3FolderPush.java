package com.s3content.push;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.s3content.util.CommonUtility;

public class AmazonS3FolderPush {

	private static AmazonS3  s3client;
	private static String bucketName = "connect-nonprod.mheducation.com";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String accessKey = "";
		String secretKey = "";
		
		Properties properties = new Properties();
		CommonUtility commonUtility = new CommonUtility();
		properties.load(new FileReader(commonUtility.getPropertyFile("awsconfig.properties")));
		accessKey = properties.getProperty("amazons3.accesskey");
		secretKey = properties.getProperty("amazons3.secretkey");
		AWSCredentials aWSCredentials = new BasicAWSCredentials(accessKey, secretKey); 

		s3client = new AmazonS3Client(aWSCredentials);
		
		try {
            System.out.println("Listing objects");
   
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix("Media/Development/Media/test/Externalized/");
            ObjectListing objectListing;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
            		"which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}

}
