package com.s3content.util;

import java.io.File;
import org.apache.commons.lang3.StringUtils;

import com.amazons3copy.AmazonS3CopyUtility;

public class CommonUtility {
	public File getPropertyFile(String propertyFileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(propertyFileName).getFile());
		return file;
	}

	/**
	 * This method makes the target object key/path.
	 * @param sourveObjectKey
	 * @param sourceFolder
	 * @param destinationFolder
	 * @return String
	 * @throws Exception
	 */
	public static String getDestinationObjectKey(String sourveObjectKey, String sourceFolder, String destinationFolder) {
		String targetFilepath = null;//target object reference
		//perform null and blank check for source object key and source folder
		if(StringUtils.isNotBlank(sourveObjectKey) && StringUtils.isNotBlank(sourceFolder)){
			targetFilepath = sourveObjectKey.substring(sourceFolder.length());//take the remaining portion after source folder
			targetFilepath = destinationFolder + targetFilepath; //prepend destination folder to make the target key
		}	
		return targetFilepath;
	}
	
	/**
	 * This method decides from the source key whether copy process should skip this object. It does 
	 * not copy folder objects and the artifacts objects created by S3Fox for directories.
	 * @param sourveObjectKey
	 * @return
	 */
	public static boolean isCopyToBeSkipped(String sourveObjectKey) {
		boolean skip = false;
		if(StringUtils.isNotBlank(sourveObjectKey)){
			if(sourveObjectKey.endsWith("_$folder$") ){//skip those artifacts objects created by S3Fox for directories
				skip = true;
			}
			/*
			if(sourveObjectKey.endsWith("/")){//object is a folder; if u want not to copy folder; but then total number of file listed and no of file copied will mismatch 
				skip = true;
			}*/
		}
		return skip;
	}
	
	/**
	 * This method takes no of thread value as String and converts it to int
	 * @param noOfThreadString
	 * @return no of thread as int
	 * @throws Exception
	 */
	public static int getThreadValueInt(String noOfThreadString) throws Exception {
		int noOfThred = 0;
		try{
			noOfThred = Integer.parseInt(noOfThreadString);//pase to int value
		}catch(NumberFormatException nfe){
			System.out.println(AmazonS3Constant.MESSAGE_NUMBER_OF_THREAD_NAN);
		}
		return noOfThred;
	}
}
