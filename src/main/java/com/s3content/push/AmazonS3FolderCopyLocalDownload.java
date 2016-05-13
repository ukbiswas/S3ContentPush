package com.s3content.push;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.s3content.util.CommonUtility;

public class AmazonS3FolderCopyLocalDownload {
	private static AmazonS3  				sourceS3client;
	private static String    				sourceAccessKey;
	private static String    				sourceSecretKey;
	
	private static String    				destAccessKey;
	private static String    				destSecretKey;
	private static AmazonS3  				destS3client;
	private static String    				sourceBucket;
	private static String    				destinationBucket;
	private static String    				sourceFolder;
	private static String    				destinationFolder;
	private static int       				iSourceObjectLength;
	static boolean							fromDateToBeConsidered;	
	private static List<String>				failedObjectList;
	
	public static void main(String[] args) throws Exception {		
		Properties properties = new Properties();
		CommonUtility commonUtility = new CommonUtility();
		properties.load(new FileReader(commonUtility.getPropertyFile("foldercopy-localdownload.properties")));
		sourceAccessKey = properties.getProperty("amazons3.source.accesskey");
		sourceSecretKey = properties.getProperty("amazons3.source.secretkey");
		
		destAccessKey = properties.getProperty("amazons3.destination.accesskey");
		destSecretKey = properties.getProperty("amazons3.destination.secretkey");
		
		sourceBucket = properties.getProperty("amazons3.source.bucket");
		destinationBucket = properties.getProperty("amazons3.destination.bucket");
		sourceFolder = properties.getProperty("amazons3.source.folder");
		destinationFolder = properties.getProperty("amazons3.destination.folder");
		
		sourceS3client = new AmazonS3Client(new BasicAWSCredentials(sourceAccessKey, sourceSecretKey));
		destS3client = new AmazonS3Client(new BasicAWSCredentials(destAccessKey, destSecretKey));
		failedObjectList = new ArrayList<String>();
		try {
            System.out.println("Listing objects....");
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(sourceBucket).withPrefix(sourceFolder);
            ObjectListing objectListing;
            String destinationObjectKey = null;
            String sourceObjectKey = null;
            S3Object s3Object;
            ObjectMetadata objectMetadata;
            do {
                objectListing = sourceS3client.listObjects(listObjectsRequest);
                
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                	sourceObjectKey = objectSummary.getKey();
                	System.out.println("sourceObjectKey = "+sourceObjectKey);
                	if (CommonUtility.isCopyToBeSkipped(sourceObjectKey) && sourceObjectKey.endsWith("/")) {
        				continue;
        			}
                    destinationObjectKey = CommonUtility.getDestinationObjectKey(sourceObjectKey, sourceFolder, destinationFolder);
                    //s3client.copyObject(sourceBucket, sourceObjectKey, destinationBucket, destinationObjectKey);
                    System.out.println(sourceObjectKey +" is being copied to "+destinationObjectKey);
                    try {
                    	s3Object = sourceS3client.getObject(sourceBucket, sourceObjectKey);
                        objectMetadata = s3Object.getObjectMetadata();
                        destS3client.putObject(destinationBucket, destinationObjectKey, s3Object.getObjectContent(), objectMetadata);
                        s3Object.close();
                        iSourceObjectLength++;
                    } catch (Exception copyException){
                    	failedObjectList.add(sourceObjectKey);
                    	System.out.println("copy failed for sourceObjectKey: "+sourceObjectKey+ ", destinationObjectKey: "+destinationObjectKey);
                    	copyException.printStackTrace();
                    }
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
            
            System.out.println("Number of copied object="+iSourceObjectLength);
            if (!failedObjectList.isEmpty()) {
				System.out.println("Objects failed to copy :");
				for (String failedObjectKey :  failedObjectList) {
					System.out.println("\n\t : " + failedObjectKey);
				}
			}

		} catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("\n\tHTTP Status Code: " + ase.getStatusCode());
            System.out.println("\n\tAWS Error Code:   " + ase.getErrorCode());
            System.out.println("\n\tError Type:       " + ase.getErrorType());
            System.out.println("\n\tRequest ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "an internal error while trying to communicate with S3, such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (Exception ex) {
        	System.out.println("Exception occurred "+ex.getMessage());
        	ex.printStackTrace();
        }
	}
}
