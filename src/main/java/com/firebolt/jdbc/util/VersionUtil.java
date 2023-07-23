package com.firebolt.jdbc.util;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.Attributes.Name;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
@CustomLog
public class VersionUtil {

	private static final Pattern VERSION_PATTERN = Pattern.compile("^\\s*(\\d+)\\.(\\d+).*");
	private static final String IMPLEMENTATION_TITLE = "Implementation-Title";
	private static final String IMPLEMENTATION_VERSION = "Implementation-Version";
	private static final String FIREBOLT_IMPLEMENTATION_TITLE = "Firebolt JDBC driver"; // This value must be the same as one defined in build.gradle/jar/manifest/attributes

	private static String driverVersion;

	static {
		try {
			driverVersion = retrieveVersion();
			log.info("Firebolt driver version used: {}", driverVersion);
		} catch (IOException e) {
			log.error("Could not get Project Version defined in the build.gradle file", e);
		}
	}

	private static String retrieveVersion() throws IOException {
		for(Enumeration<URL> eurl = Thread.currentThread().getContextClassLoader().getResources("META-INF/MANIFEST.MF"); eurl.hasMoreElements();) {
			URL url = eurl.nextElement();
			try (InputStream in = url.openStream()) {
				Manifest manifest = new Manifest(in);
				String implementationTitle = (String)manifest.getMainAttributes().get(new Name(IMPLEMENTATION_TITLE));
				if (FIREBOLT_IMPLEMENTATION_TITLE.equals(implementationTitle)) {
					return (String)manifest.getMainAttributes().get(new Name(IMPLEMENTATION_VERSION));
				}
			}
		}
		Properties properties = new Properties();
		properties.load(new FileInputStream("gradle.properties"));
		return properties.getProperty("version");
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
