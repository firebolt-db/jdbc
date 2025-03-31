package com.firebolt.jdbc.cache;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Based on different operating systems, it resolves the path to the directory where an application can write to disk
 */
public class DirectoryPathResolver {

    private static final String FIREBOLT_JDBC_APP_NAME = "fireboltDriverJdbc";

    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String OS_NAME_PROPERTY = "os.name";

    public Path resolveFireboltJdbcDirectory() {
        String userHome = System.getProperty(USER_HOME_PROPERTY);
        Path appDataPath;

        String osName = System.getProperty(OS_NAME_PROPERTY).toLowerCase();

        if (osName.contains("win")) {
            appDataPath = Paths.get(System.getenv("APPDATA"), FIREBOLT_JDBC_APP_NAME);
        } else if (osName.contains("mac")) {
            appDataPath = Paths.get(userHome, "Library", "Application Support", FIREBOLT_JDBC_APP_NAME);
        } else {
            appDataPath = Paths.get(userHome, ".config", FIREBOLT_JDBC_APP_NAME);
        }

        return appDataPath;
    }

}
