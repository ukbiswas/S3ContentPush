package com.s3content.util;

import java.io.File;
import org.apache.commons.lang3.StringUtils;

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
}
