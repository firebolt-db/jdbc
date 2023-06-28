package com.firebolt.jdbc.client;

public class UserAgentFormatter {
    private static final String VERSION = DriverVersionRetriever.getDriverVersion();

    public static String userAgent(String format) {
        return userAgent(format, VERSION, javaVersion(), osName(), osVersion());
    }

    public static String userAgent(String format, String driverVersion, String javaVersion, String osName, String osVersion) {
        return String.format(format, driverVersion, javaVersion, osName, osVersion);
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
