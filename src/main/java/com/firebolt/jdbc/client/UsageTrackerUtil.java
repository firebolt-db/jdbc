package com.firebolt.jdbc.client;

import com.firebolt.jdbc.util.VersionUtil;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@UtilityClass
public class UsageTrackerUtil {

	private static final Logger log = Logger.getLogger(UsageTrackerUtil.class.getName());
	public static final Map<String, String> CLIENT_MAP = Map.of(
			"Tableau", "com.tableau",
			"Looker", "com.looker",
			"Calcite", "org.apache.calcite",
			"Metabase", "metabase");

	public static final Map<String, String> DRIVER_MAP = Map.of();

	private static String getVersionForClass(String name) {
		try {
			Class<?> c = Class.forName(name);
			return c.getPackage().getImplementationVersion();
		} catch (ClassNotFoundException e) {
			log.log(Level.FINE, "Unable to get version for class {0}", name);
			return "";
		}
	}

	public Map<String, String> getClients(StackTraceElement[] stack, Map<String, String> clientMap) {
		Map<String, String> clients = new HashMap<>();
		if (stack == null) {
			return clients;
		}
		for (StackTraceElement s : stack) {
			for (Map.Entry<String, String> connectorEntry : clientMap.entrySet()) {
				if (s.getClassName().contains(connectorEntry.getValue())) {
					String version = getVersionForClass(s.getClassName());
					log.log(Level.FINE, "Detected running from {0} Version {1}", new Object[] {connectorEntry.getKey(), version});
					clients.put(connectorEntry.getKey(), version);
				}
			}
		}
		if (clients.isEmpty()) {
			log.log(Level.FINE, "No clients detected for tracking");
		}
		return clients;
	}

	private static Map<String, String> extractNameToVersion(String namesAndVersions) {
		// Example: connectors=ConnA:1.0.2,ConnB:2.9.3
		Map<String, String> nameToVersion = new HashMap<>();
		if (namesAndVersions.matches("(\\w+:\\d+?\\.\\d+?\\.\\d+?,?){1,100}")) {
			for (String connector : namesAndVersions.split(",")) {
				String[] connectorInfo = connector.split(":");
				// Name, Version
				nameToVersion.put(connectorInfo[0], connectorInfo[1]);
			}
		} else {
			log.log(Level.FINE, "Incorrect connector format is provided: {0}, Expected: ConnA:1.0.2,ConnB:2.9.3", namesAndVersions);
		}
		return nameToVersion;
	}

	private static String mapToString(Map<String, String> map) {
		StringBuilder connectorString = new StringBuilder();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			connectorString.append(entry.getKey());
			connectorString.append("/");
			connectorString.append(entry.getValue());
			connectorString.append(" ");
		}
		return connectorString.toString().trim();
	}

	public static String getUserAgentString(String userDrivers, String userClients) {
		Map<String, String> detectedDrivers = getClients(Thread.currentThread().getStackTrace(), DRIVER_MAP);
		Map<String, String> detectedClients = getClients(Thread.currentThread().getStackTrace(), CLIENT_MAP);
		detectedDrivers.putAll(extractNameToVersion(userDrivers));
		detectedClients.putAll(extractNameToVersion(userClients));
		String javaVersion = System.getProperty("java.version");
		String systemVersion = System.getProperty("os.version");

		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("win")) {
			os = "Windows";
		} else if (os.contains("mac")) {
			// Keeping this in sync with Python counterpart
			os = "Darwin";
		} else if (os.contains("linux")) {
			os = "Linux";
		}

		String result = mapToString(detectedClients) + " JDBC/" + VersionUtil.getDriverVersion() + " (Java "
				+ javaVersion + "; " + os + " " + systemVersion + "; )" + " " + mapToString(detectedDrivers);
		return result.trim();
	}
}
