package com.s3content.push;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
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

public class AmazonS3FolderCopy extends Thread {
	private static AmazonS3  				s3client;
	private static String    				accessKey;
	private static String    				secretKey;
	private static String    				sourceBucket;
	private static String    				destinationBucket;
	private static String    				sourceFolder;
	private static String    				destinationFolder;
	private static int       				noOfThread;
	private static int       				iSourceObjectLength;
	private static List<String>	            sourceS3ObjectKeyList;
	private static List<String>				failedObjectList;
	
	public static void main(String[] args) throws Exception {		
		Properties properties = new Properties();
		CommonUtility commonUtility = new CommonUtility();
		properties.load(new FileReader(commonUtility.getPropertyFile("AmazonS3FolderCopy.properties")));
		accessKey = properties.getProperty("amazons3.accesskey");
		secretKey = properties.getProperty("amazons3.secretkey");
		sourceBucket = properties.getProperty("amazons3.source.bucket");
		destinationBucket = properties.getProperty("amazons3.destination.bucket");
		sourceFolder = properties.getProperty("amazons3.source.folder");
		destinationFolder = properties.getProperty("amazons3.destination.folder");
		
		noOfThread = CommonUtility.getThreadValueInt(properties.getProperty("amazons3.folderCopy.noOfThread"));
		
		AWSCredentials aWSCredentials = new BasicAWSCredentials(accessKey, secretKey); 
		s3client = new AmazonS3Client(aWSCredentials);
		sourceS3ObjectKeyList = new ArrayList<String>();
		failedObjectList = new ArrayList<String>();
		try {
            System.out.println("Listing objects....");
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(sourceBucket).withPrefix(sourceFolder);
            ObjectListing objectListing;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    sourceS3ObjectKeyList.add(objectSummary.getKey());
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
            
            iSourceObjectLength = sourceS3ObjectKeyList.size() - 1;
            System.out.println("size of sourceS3ObjectKeyList="+sourceS3ObjectKeyList.size());
            System.out.println("iSourceObjectLength="+iSourceObjectLength);
            
            for (int iThreadCounter=0; iThreadCounter < noOfThread; iThreadCounter++ ) {
            	AmazonS3FolderCopy amazonS3FolderCopy = new AmazonS3FolderCopy();
            	amazonS3FolderCopy.start();
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

	public void run() {
		System.out.println("## Copy process going on....in :"+this.getName());
		int noOfCopiedFiles = 0;//holds no of copied file for this thread
		String sourceObjectKey = null;
		String destinationObjectKey = null;
		while (iSourceObjectLength >= 0) {
			synchronized (sourceS3ObjectKeyList) {
				sourceObjectKey = sourceS3ObjectKeyList.get(iSourceObjectLength);
				iSourceObjectLength = iSourceObjectLength - 1;
			}
			//if the copy process needs to skip this objects
			if (CommonUtility.isCopyToBeSkipped(sourceObjectKey)) {
				continue;
			}
			
			try {
				destinationObjectKey = CommonUtility.getDestinationObjectKey(sourceObjectKey, sourceFolder, destinationFolder);
				System.out.println("sourceObjectKey : "+sourceObjectKey+", destinationObjectKey :"+destinationObjectKey);
				s3client.copyObject(sourceBucket, sourceObjectKey, destinationBucket, destinationObjectKey);
				noOfCopiedFiles++;
			} catch (AmazonServiceException ase) {
				ase.printStackTrace();
	            System.out.println("Error Message:    " + ase.getMessage()
	            				   + "\n\tHTTP Status Code: " + ase.getStatusCode()
	            				   + "\n\tAWS Error Code:   " + ase.getErrorCode()
	            				   + "\n\tError Type:       " + ase.getErrorType()
	            				   + "\n\tRequest ID:       " + ase.getRequestId());
	            failedObjectList.add(sourceObjectKey);
	        } catch (AmazonClientException ace) {
	            System.out.println("Error Message: " + ace.getMessage());
	            failedObjectList.add(sourceObjectKey);
	        } catch (Exception ex) {
	        	System.out.println("Exception occurred "+ex.getMessage());
	        	ex.printStackTrace();
	        }
			
			if (iSourceObjectLength == 0 && !failedObjectList.isEmpty()) {
				System.out.println("Objects failed to copy :");
				for (String failedObjectKey :  failedObjectList) {
					System.out.println("\n\t : " + failedObjectKey);
				}
			}
		}
		System.out.println("Number of objects copied by "+this.getName() +" : "+noOfCopiedFiles);
	}
}
