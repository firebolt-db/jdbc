package com.firebolt.jdbc.cache;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Based on different operating systems, it resolves the path to the directory where an application can write to disk
 */
public class DirectoryPathResolver {

    private static final String FIREBOLT_JDBC_APP_NAME = "fireboltDriverJdbc";

    public Path resolveFireboltJdbcDirectory() {
        String userHome = System.getProperty("user.home");
        Path appDataPath;

        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            appDataPath = Paths.get(System.getenv("APPDATA"), FIREBOLT_JDBC_APP_NAME);
        } else if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            appDataPath = Paths.get(userHome, "Library", "Application Support", FIREBOLT_JDBC_APP_NAME);
        } else {
            appDataPath = Paths.get(userHome, ".config", FIREBOLT_JDBC_APP_NAME);
        }

        return appDataPath;
    }

}
