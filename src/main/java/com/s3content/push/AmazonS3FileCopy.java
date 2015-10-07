package com.s3content.push;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import com.s3content.util.AmazonS3Constant;
import com.s3content.util.CommonUtility;

public class AmazonS3FileCopy {
	private static AmazonS3  				s3client;
	private static String    				accessKey;
	private static String    				secretKey;
	private static String    				sourceBucket;
	private static String    				destinationBucket;
	private static String    				sourceFolder;
	private static String    				destinationFolder;
	private static int       				iSourceObjectLength;
	private static Date      				fromDate;
	static boolean							fromDateToBeConsidered;	
	private static List<S3ObjectSummary>	sourceS3ObjectKeyList;
	private static List<String>				failedObjectList;
	
	public static void main(String[] args) throws Exception {		
		Properties properties = new Properties();
		CommonUtility commonUtility = new CommonUtility();
		properties.load(new FileReader(commonUtility.getPropertyFile("awsconfig.properties")));
		accessKey = properties.getProperty("amazons3.accesskey");
		secretKey = properties.getProperty("amazons3.secretkey");
		sourceBucket = properties.getProperty("amazons3.source.bucket");
		destinationBucket = properties.getProperty("amazons3.destination.bucket");
		sourceFolder = properties.getProperty("amazons3.source.folder");
		destinationFolder = properties.getProperty("amazons3.destination.folder");
		
		String fromDateString = properties.getProperty("amazons3.folderCopy.fromDate");
		
		AWSCredentials aWSCredentials = new BasicAWSCredentials(accessKey, secretKey); 
		s3client = new AmazonS3Client(aWSCredentials);
		sourceS3ObjectKeyList = new ArrayList<S3ObjectSummary>();
		failedObjectList = new ArrayList<String>();
		try {
            System.out.println("Listing objects....");
            validateFromDatePropertyValue(fromDateString);
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(sourceBucket).withPrefix(sourceFolder);
            ObjectListing objectListing;
            //String destinationObjectKey = null;
            //String sourceObjectKey = null;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                /*
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                	sourceS3ObjectKeyList.add(objectSummary.getKey());
                    System.out.println(" - " + sourceObjectKey + "  " + "(size = " + objectSummary.getSize() + ")");
                    destinationObjectKey = CommonUtility.getDestinationObjectKey(sourceObjectKey, sourceFolder, destinationFolder);
                    s3client.copyObject(sourceBucket, sourceObjectKey, destinationBucket, destinationObjectKey);
                    System.out.println("Object "+sourceObjectKey +" copied to "+destinationObjectKey);
                }*/
                sourceS3ObjectKeyList.addAll(objectListing.getObjectSummaries());
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
            
            iSourceObjectLength = sourceS3ObjectKeyList.size() - 1;
            System.out.println("size of sourceS3ObjectKeyList="+sourceS3ObjectKeyList.size());
            System.out.println("iSourceObjectLength="+iSourceObjectLength);

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
		int noOfCopiedFiles = 0;//holds no of copied file for this thread
		String sourceObjectKey = null;
		String destinationObjectKey = null;
		S3ObjectSummary s3ObjectSummary = null;
		while (iSourceObjectLength >= 0) {
			synchronized (sourceS3ObjectKeyList) {
				s3ObjectSummary = sourceS3ObjectKeyList.get(iSourceObjectLength);
				iSourceObjectLength = iSourceObjectLength - 1;
			}
			
			sourceObjectKey = s3ObjectSummary.getKey();
			//if the copy process needs to skip this objects
			if (CommonUtility.isCopyToBeSkipped(sourceObjectKey)) {
				continue;
			}
			
			try {
				destinationObjectKey = CommonUtility.getDestinationObjectKey(sourceObjectKey, sourceFolder, destinationFolder);
				s3client.copyObject(sourceBucket, sourceObjectKey, destinationBucket, destinationObjectKey);
				//System.out.println("## "+sourceObjectKey);
				noOfCopiedFiles++;
			} catch (AmazonServiceException ase) {
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
	}
	
	/**
	 * This method validates from date. if the date is not parse able it returns false.
	 * Also it sets a flag to indicate if the copy process should use from date limit. 
	 * @return boolean
	 * @throws Exception
	 */
	public static boolean validateFromDatePropertyValue(String dateInStringFormat) throws Exception {
		boolean isFromDateValid = true;
		try {
			SimpleDateFormat date_format = new SimpleDateFormat("dd/MM/yyyy");
			fromDate = date_format.parse(dateInStringFormat);
			System.out.println("fromDate="+fromDate);
			Calendar calDate = Calendar.getInstance();
			calDate.setTime(fromDate);
			System.out.println("calDate="+calDate);
			fromDateToBeConsidered = true;//this indicates if from date to be considered
		} catch(Exception ex) {
			isFromDateValid = false;
			System.out.println(AmazonS3Constant.MESSAGE_EXCEPTION_DATE_FORMATING);
		}
		return isFromDateValid;
	}
}
