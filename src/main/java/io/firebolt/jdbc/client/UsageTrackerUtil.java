package io.firebolt.jdbc.client;

import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.ProjectVersionUtil;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@UtilityClass
public class UsageTrackerUtil {

  public static final Map<String, String> CLIENT_MAP =
      ImmutableMap.of(
          "Tableau", "com.tableau",
          "Looker", "com.looker",
          "Calcite", "org.apache.calcite",
          "Metabase", "metabase");

  public static final Map<String, String> DRIVER_MAP = ImmutableMap.of();

  private static String getVersionForClass(String name) {
    try {
      Class c = Class.forName(name);
      return c.getPackage().getImplementationVersion();
    } catch (ClassNotFoundException e) {
      log.debug("Unable to get version for class " + name);
      return "";
    }
  }

  public Map<String, String> getClients(StackTraceElement[] stack, Map<String, String> clientMap) {
    Map<String, String> clients = new HashMap<String, String>();
    if (stack == null) {
      return clients;
    }
    for (StackTraceElement s : stack) {
      for (String connector : clientMap.keySet()) {
        if (s.getClassName().contains(clientMap.get(connector))) {
          String version = getVersionForClass(s.getClassName());
          log.debug("Detected running from " + connector + " Version " + version);
          clients.put(connector, version);
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
      log.info(
          "Incorrect connector format is provided: "
              + overrides
              + " Expected: ConnA:1.0.2,ConnB:2.9.3");
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
    Map<String, String> detected_drivers =
        getClients(Thread.currentThread().getStackTrace(), DRIVER_MAP);
    Map<String, String> detected_clients =
        getClients(Thread.currentThread().getStackTrace(), CLIENT_MAP);
    overrideClients(detected_drivers, userDrivers);
    overrideClients(detected_clients, userClients);
    String javaVersion = System.getProperty("java.version");
    String systemVersion = System.getProperty("os.version");

    String os = System.getProperty("os.name").toLowerCase();
    if (os.indexOf("win") >= 0) {
      os = "Windows";
    } else if (os.indexOf("mac") >= 0) {
      // Keeping this in sync with Python counterpart
      os = "Darwin";
    } else if (os.indexOf("linux") >= 0) {
      os = "Linux";
    }

    String result =
        mapToString(detected_clients)
            + " JDBC/"
            + ProjectVersionUtil.getProjectVersion()
            + " (Java "
            + javaVersion
            + "; "
            + os
            + " "
            + systemVersion
            + "; )"
            + " "
            + mapToString(detected_drivers);
    return result.trim();
  }
}
