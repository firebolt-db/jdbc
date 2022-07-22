package com.firebolt.jdbc;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
@Slf4j
public class VersionUtil {

  private static final Pattern VERSION_PATTERN = Pattern.compile("^\\s*(\\d+)\\.(\\d+).*");

  private static String driverVersion;

  static {
    Properties properties = new Properties();
    driverVersion = null;
    try {
      properties.load(VersionUtil.class.getResourceAsStream("/config.properties"));
      driverVersion = properties.getProperty("version");
    } catch (IOException e) {
      log.error("Could not get Project Version defined in the build.gradle file", e);
    }
  }

  public static int getMajorDriverVersion() {
    return extractMajorVersion(driverVersion);
  }

  public static int getDriverMinorVersion() {
    return extractMinorVersion(driverVersion);
  }

  public static int extractMajorVersion(String version) {
    if (version == null) {
      return 0;
    }
    Matcher matcher = VERSION_PATTERN.matcher(version);
    return matcher.matches() ? Integer.parseInt(matcher.group(1)) : 0;
  }

  public static int extractMinorVersion(String version) {
    if (version == null) {
      return 0;
    }
    Matcher matcher = VERSION_PATTERN.matcher(version);
    return matcher.matches() ? Integer.parseInt(matcher.group(2)) : 0;
  }

  public static String getDriverVersion() {
    return driverVersion;
  }
}
