package com.s3content.push;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.s3content.util.CommonUtility;

public class AmazonS3FileCopyLocalDownload {
	private static AmazonS3  				sourceS3client;
	private static String    				sourceAccessKey;
	private static String    				sourceSecretKey;
	
	private static String    				destAccessKey;
	private static String    				destSecretKey;
	private static AmazonS3  				destS3client;
	private static String    				sourceBucket;
	private static String    				destinationBucket;
	private static String    				sourceFolder;
	private static String    				destFolder;
	private static int       				iSourceObjectLength;
	static boolean							fromDateToBeConsidered;	
	private static List<String>				failedObjectList;
	
	public static void main(String[] args) throws Exception {		
		Properties properties = new Properties();
		CommonUtility commonUtility = new CommonUtility();
		properties.load(new FileReader(commonUtility.getPropertyFile("filecopy-localdownload.properties")));
		sourceAccessKey = properties.getProperty("amazons3.source.accesskey");
		sourceSecretKey = properties.getProperty("amazons3.source.secretkey");
		
		destAccessKey = properties.getProperty("amazons3.destination.accesskey");
		destSecretKey = properties.getProperty("amazons3.destination.secretkey");
		
		sourceBucket = properties.getProperty("amazons3.source.bucket");
		destinationBucket = properties.getProperty("amazons3.destination.bucket");
		sourceFolder = properties.getProperty("amazons3.source.folder");
		destFolder = properties.getProperty("amazons3.destination.folder");
		
		sourceS3client = new AmazonS3Client(new BasicAWSCredentials(sourceAccessKey, sourceSecretKey));
		destS3client = new AmazonS3Client(new BasicAWSCredentials(destAccessKey, destSecretKey));
		failedObjectList = new ArrayList<String>();
		BufferedReader bufferedReader = null;
		try {
            System.out.println("Coping files....");
            bufferedReader = new BufferedReader(new FileReader(commonUtility.getPropertyFile("inputFiles.txt")));
            
            String destinationObjectKey = null;
            String sourceObjectKey = null;
            S3Object s3Object = null;
            ObjectMetadata objectMetadata;
            
            while ((sourceObjectKey = bufferedReader.readLine()) != null) {
            	
            	try {
            		s3Object = sourceS3client.getObject(sourceBucket, sourceObjectKey);
            		destinationObjectKey = sourceObjectKey.replace(sourceFolder, destFolder);
            		System.out.println("## destinationObjectKey="+destinationObjectKey);
                    objectMetadata = s3Object.getObjectMetadata();
                    destS3client.putObject(destinationBucket, destinationObjectKey, s3Object.getObjectContent(), objectMetadata);
                    iSourceObjectLength++;
                } catch (Exception copyException) {
                	failedObjectList.add(sourceObjectKey);
                 	System.out.println("copy failed for sourceObjectKey: "+sourceObjectKey+ ", destinationObjectKey: "+destinationObjectKey);
                 	copyException.printStackTrace();
                } finally {
                	if(s3Object != null) {
                		s3Object.close();
                	}
				}
            }
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
        } finally {
			if(bufferedReader != null) {
				bufferedReader.close();
			}
		}
	}
}
