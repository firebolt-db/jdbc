package com.firebolt.jdbc.client;

import java.io.IOException;
import java.util.Properties;

public class DriverVersionRetriever {
    private static final String driverVersion;

    static {
        try {
            Properties properties = new Properties();
            properties.load(DriverVersionRetriever.class.getResourceAsStream("/version.properties"));
            driverVersion = properties.getProperty("version");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot retrieve driver version");
        }
    }

    public static String getDriverVersion() {
        return driverVersion;
    }
}
