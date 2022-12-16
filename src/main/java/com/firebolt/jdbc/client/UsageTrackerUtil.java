package com.firebolt.jdbc.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.VersionUtil;
import com.google.common.collect.ImmutableMap;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

@CustomLog
@UtilityClass
public class UsageTrackerUtil {

	public static final Map<String, String> CLIENT_MAP = ImmutableMap.of("Tableau", "com.tableau", "Looker",
			"com.looker", "Calcite", "org.apache.calcite", "Metabase", "metabase");

	public static final Map<String, String> DRIVER_MAP = ImmutableMap.of();

	private static String getVersionForClass(String name) {
		try {
			Class<?> c = Class.forName(name);
			return c.getPackage().getImplementationVersion();
		} catch (ClassNotFoundException e) {
			log.debug("Unable to get version for class " + name);
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
				if (StringUtils.contains(s.getClassName(), connectorEntry.getValue())) {
					String version = getVersionForClass(s.getClassName());
					log.debug("Detected running from " + connectorEntry.getKey() + " Version " + version);
					clients.put(connectorEntry.getKey(), version);
				}
			}
		}
		if (clients.isEmpty()) {
			log.debug("No clients detected for tracking");
		}
		return clients;
	}

	private static void overrideClients(Map<String, String> clients, String overrides) {
		// Example: connectors=ConnA:1.0.2,ConnB:2.9.3
		if (overrides.matches("(\\w+:\\d+?\\.\\d+?\\.\\d+?,?)+")) {
			for (String connector : overrides.split(",")) {
				String[] connectorInfo = connector.split(":");
				// Name, Version
				clients.put(connectorInfo[0], connectorInfo[1]);
			}
		} else {
			log.debug("Incorrect connector format is provided: " + overrides + " Expected: ConnA:1.0.2,ConnB:2.9.3");
		}
	}

	private static String mapToString(Map<String, String> map) {
		StringBuilder connectorString = new StringBuilder();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			connectorString.append(entry.getKey() + "/" + entry.getValue());
			connectorString.append(" ");
		}
		return connectorString.toString().trim();
	}

	public static String getUserAgentString(String userDrivers, String userClients) {
		Map<String, String> detectedDrivers = getClients(Thread.currentThread().getStackTrace(), DRIVER_MAP);
		Map<String, String> detectedClients = getClients(Thread.currentThread().getStackTrace(), CLIENT_MAP);
		overrideClients(detectedDrivers, userDrivers);
		overrideClients(detectedClients, userClients);
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
