package com.firebolt.jdbc.client;

import java.util.Optional;

public class UserAgentFormatter {
    private static final String VERSION = DriverVersionRetriever.getDriverVersion();

    public static String userAgent(String format) {
        return userAgent(format, VERSION, javaVersion(), osName(), osVersion(), Optional.empty());
    }

    public static String userAgent(String format, Optional<String> connectionInfo) {
        return userAgent(format, VERSION, javaVersion(), osName(), osVersion(), connectionInfo);
    }

    public static String userAgent(String format, String driverVersion, String javaVersion, String osName, String osVersion, Optional<String> connectionInfo) {
        // Mac OS is renamed to Darwin in the user agent string
        String userAgentString =  String.format(format, driverVersion, javaVersion, osName, osVersion).replace("Mac OS X", "Darwin");
        return connectionInfo.isEmpty() ? userAgentString : userAgentString + connectionInfo.get();
    }

    public static String osName() {
        return System.getProperty("os.name");
    }

    public static String osVersion() {
        return System.getProperty("os.version");
    }

    public static String javaVersion() {
        return System.getProperty("java.version");
    }
}
