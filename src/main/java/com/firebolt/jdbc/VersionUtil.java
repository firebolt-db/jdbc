package com.firebolt.jdbc;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
@CustomLog
public class VersionUtil {

	private static final Pattern VERSION_PATTERN = Pattern.compile("^\\s*(\\d+)\\.(\\d+).*");

	private static String driverVersion;

	static {
		Properties properties = new Properties();
		driverVersion = null;
		try {
			properties.load(VersionUtil.class.getResourceAsStream("/version.properties"));
			driverVersion = properties.getProperty("version");
			log.info("Firebolt driver version used: {}", driverVersion);
		} catch (IOException e) {
			log.error("Could not get Project Version defined in the build.gradle file", e);
		}
	}

	/**
	 * Returns the driver major version
	 * 
	 * @return the driver major version
	 */
	public int getMajorDriverVersion() {
		return extractMajorVersion(driverVersion);
	}

	/**
	 * Returns the driver minor version
	 * 
	 * @return the driver minor version
	 */
	public int getDriverMinorVersion() {
		return extractMinorVersion(driverVersion);
	}

	/**
	 * Extracts the major version from the version provided
	 * 
	 * @param version the version to extract the major version from
	 * @return the major version
	 */
	public int extractMajorVersion(String version) {
		if (version == null) {
			return 0;
		}
		Matcher matcher = VERSION_PATTERN.matcher(version);
		return matcher.matches() ? Integer.parseInt(matcher.group(1)) : 0;
	}

	/**
	 * Extracts the minor version from the version provided
	 * 
	 * @param version the version to extract the minor version from
	 * @return the minor version
	 */
	public int extractMinorVersion(String version) {
		if (version == null) {
			return 0;
		}
		Matcher matcher = VERSION_PATTERN.matcher(version);
		return matcher.matches() ? Integer.parseInt(matcher.group(2)) : 0;
	}

	/**
	 * Returns the driver version
	 * 
	 * @return the driver version
	 */
	public String getDriverVersion() {
		return driverVersion;
	}
}
