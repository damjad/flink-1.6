package org.apache.flink.morpheus.wordcount.utils;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

/**
 * It handle the creation and loading of the properties given a property file.
 */
public class PropertiesHandler {
	private static PropertiesHandler propertiesHandler;

	private Properties moduleProperties;
	private String filePath;

	public String getFilePath() {
		return filePath;
	}

	private PropertiesHandler(String filePath) throws IOException {
		this.filePath = filePath;
		loadFromFile(filePath);
	}

	private PropertiesHandler() {
		moduleProperties = new Properties();
	}

	/**
	 * Return the singleton instance of for class {@link PropertiesHandler} using property
	 *
	 * @param filePath
	 * @return
	 */
	public static PropertiesHandler getInstance(String filePath) throws IOException {

		if (propertiesHandler == null) {
			synchronized (PropertiesHandler.class) {
				if (propertiesHandler == null) {
					if (filePath == null) {
						propertiesHandler = new PropertiesHandler();
					}
					else {
						propertiesHandler = new PropertiesHandler(filePath);
					}
				}
			}
		}
		return propertiesHandler;
	}

	public static PropertiesHandler getInstance() {
		return propertiesHandler;
	}

	public Properties getModuleProperties() {
		return moduleProperties;
	}

	public void setModuleProperties(Properties sysProperties) {
		moduleProperties = sysProperties;
	}

	public String getProperty(String key, String defaultValue) {
		return moduleProperties.getProperty(key, defaultValue);
	}

	public String getProperty(String key) {
		return moduleProperties.getProperty(key);
	}

	public Integer getInteger(String key) {
		return getInteger(key, null);
	}

	public Integer getInteger(String key, Integer defaultValue) {
		String val = moduleProperties.getProperty(key);
		if (val == null) {
			return defaultValue;
		}
		return Integer.valueOf(val);
	}

	public Double getDouble(String key) {
		return getDouble(key, null);
	}

	public Double getDouble(String key, Double defaultValue) {
		String val = moduleProperties.getProperty(key);
		if (val == null) {
			return defaultValue;
		}
		return Double.valueOf(val);
	}

	public Long getLong(String key) {
		return getLong(key, null);
	}

	public Boolean getBoolean(String key) {
		return getBoolean(key, null);
	}

	public Boolean getBoolean(String key, Boolean defaultValue) {
		String val = moduleProperties.getProperty(key);
		if (val == null) {
			return defaultValue;
		}
		return Boolean.valueOf(val);
	}

	public Long getLong(String key, Long defaultValue) {
		String val = moduleProperties.getProperty(key);
		if (val == null) {
			return defaultValue;
		}
		return Long.valueOf(val);
	}

	public void loadFromFile(String filePath) throws IOException {

		if (null == filePath) {
			InputStream is = getClass().getResourceAsStream("/module-config.properties");
			if (is == null) {
				throw new FileNotFoundException("Could not read properties file from classpath: module-config.properties");
			}

			moduleProperties = new Properties();
			moduleProperties.load(is);
		}
		else {
			File propertiesFile;
			propertiesFile = new File(filePath);
			if (!(propertiesFile.exists() && propertiesFile.canRead())) {
				throw new FileNotFoundException("Unable to read file : " + propertiesFile.getPath());
			}

			moduleProperties = new Properties();
			moduleProperties.load(new FileInputStream(propertiesFile));
		}

	}


	public int[] getIntegerArray(String key, int[] parallelisms) {
		if(!moduleProperties.containsKey(key)) {
			return parallelisms;
		}
		return getIntegerArray(key);
	}

	public int[] getIntegerArray(String key) {
		return Arrays.stream(moduleProperties.getProperty(key).split(",")).mapToInt(Integer::parseInt).toArray();
	}
}
