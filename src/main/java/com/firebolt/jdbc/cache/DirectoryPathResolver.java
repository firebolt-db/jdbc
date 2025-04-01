package com.firebolt.jdbc.cache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

/**
 * Based on different operating systems, it resolves the path to the directory where an application can write to disk
 */
@CustomLog
public class DirectoryPathResolver {

    private static final String FIREBOLT_JDBC_APP_NAME = "fireboltDriverJdbc";

    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String OS_NAME_PROPERTY = "os.name";
    public static final String TEMP_DIRECTORY_PROPERTY = "java.io.tmpdir";
    /**
     * We will default to writing the connection cache data in the default temp directory for each operating system
     * @return
     */
    public Path resolveFireboltJdbcDirectory() {
        Path cacheDirectoryPath;

        String osName = System.getProperty(OS_NAME_PROPERTY).toLowerCase();

        if (osName.contains("win")) {
            cacheDirectoryPath = Paths.get(System.getProperty(TEMP_DIRECTORY_PROPERTY), FIREBOLT_JDBC_APP_NAME);
        } else if (osName.contains("mac")) {
            // this is per user and will be deleted upon reboot or when the OS needs more disk space
            String tempDirectory = System.getenv("TMPDIR");
            cacheDirectoryPath = Path.of(tempDirectory, FIREBOLT_JDBC_APP_NAME);
        } else {
            // for linux try the user specific temp directory. If that is not set then use the /tmp
            String tempUserDirectory = System.getenv("XDG_RUNTIME_DIR");
            if (StringUtils.isBlank(tempUserDirectory)) {
                tempUserDirectory = Path.of("/tmp", System.getProperty(USER_HOME_PROPERTY)).toString(); // Fallback if not set
            }
            cacheDirectoryPath = Path.of(tempUserDirectory, FIREBOLT_JDBC_APP_NAME);
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
