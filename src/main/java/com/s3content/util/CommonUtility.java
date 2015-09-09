package com.s3content.util;

import java.io.File;
import java.io.FileReader;

public class CommonUtility {
	public File getPropertyFile(String propertyFileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(propertyFileName).getFile());
		return file;
	}

}
