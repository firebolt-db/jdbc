package io.firebolt.jdbc.client;

import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.ProjectVersionUtil;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
public class UsageTrackerUtil {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(UsageTrackerUtil.class);

  private static final Map<String, String> CLIENT_MAP =
      ImmutableMap.of(
          "Tableau", "com.tableau",
          "Looker", "com.looker",
          "Calcite", "org.apache.calcite",
          "Metabase", "metabase");

  private static String getVersionForClass(String name) {
    try {
      Class c = Class.forName(name);
      return c.getPackage().getImplementationVersion();
    } catch (ClassNotFoundException e) {
      log.debug("Unable to get version for class " + name);
      return "";
    }
  }

  public Map<String, String> getClients(StackTraceElement[] stack) {
    Map<String, String> clients = new HashMap<String, String>();
    if (stack == null) {
      return clients;
    }
    for (StackTraceElement s : stack) {
      for (String connector : CLIENT_MAP.keySet()) {
        if (s.getClassName().contains(CLIENT_MAP.get(connector))) {
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

  public static String getUserAgentString(String userConnectors) {
    Map<String, String> detected_connectors = getClients(Thread.currentThread().getStackTrace());
    overrideClients(detected_connectors, userConnectors);
    String version = System.getProperty("java.version");
    String system_ver = System.getProperty("os.version");

    String os = System.getProperty("os.name").toLowerCase();
    if (os.indexOf("win") >= 0) {
      os = "Windows";
    } else if (os.indexOf("mac") >= 0) {
      // Keeping this in sync with Python counterpart
      os = "Darwin";
    } else if (os.indexOf("linux") >= 0) {
      os = "Linux";
    }

    StringBuilder connectorString = new StringBuilder();
    for (Map.Entry<String, String> entry : detected_connectors.entrySet()) {
      connectorString.append(entry.getKey() + "/" + entry.getValue());
      connectorString.append(" ");
    }
    return "JDBC/"
        + ProjectVersionUtil.getProjectVersion()
        + " (Java "
        + version
        + "; "
        + os
        + " "
        + system_ver
        + "; )"
        + " "
        + connectorString.toString().trim();
  }
}
