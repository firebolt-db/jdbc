package com.firebolt.jdbc.cache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.CustomLog;

/**
 * Based on different operating systems, it resolves the path to the directory where an application can write to disk
 */
@CustomLog
public class DirectoryPathResolver {

    private static final String FIREBOLT_JDBC_APP_NAME = "fireboltDriverJdbc";

    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String OS_NAME_PROPERTY = "os.name";

    /**
     * We will default to writing the connection cache data in the default temp directory for each operating system
     * @return
     */
    public Path resolveFireboltJdbcDirectory() {
        String userHome = System.getProperty(USER_HOME_PROPERTY);
        Path cacheDirectoryPath;

        String osName = System.getProperty(OS_NAME_PROPERTY).toLowerCase();

        if (osName.contains("win")) {
            cacheDirectoryPath = Paths.get(System.getenv("LOCALAPPDATA"), FIREBOLT_JDBC_APP_NAME, "cache");
        } else {
            // for mac and linux temp location is the same
            cacheDirectoryPath = Path.of(userHome, "Library", "Caches", FIREBOLT_JDBC_APP_NAME);
        }

        // Ensure cache directory exists
        try {
            Files.createDirectories(cacheDirectoryPath);
        } catch (IOException e) {
            log.error("Failed to create the cached directory.");
        }

        return cacheDirectoryPath;
    }

}
