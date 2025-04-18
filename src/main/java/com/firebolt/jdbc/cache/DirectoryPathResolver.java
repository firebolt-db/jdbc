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

    private static final String FIREBOLT_DRIVER_DIRECTORY_NAME = "fireboltDriver";

    public static final String USER_HOME_PROPERTY = "user.home";
    public static final String OS_NAME_PROPERTY = "os.name";
    public static final String TEMP_DIRECTORY_PROPERTY = "java.io.tmpdir";

    /**
     * We will default to writing the connection cache data in the default temp directory for each operating system
     * It will make sure that the directory is crated.
     * @return
     */
    public Path resolveFireboltJdbcDirectory() {
        String osName = System.getProperty(OS_NAME_PROPERTY).toLowerCase();
        Path fireboltJdbcDirectory = getDefaultDirectoryPath(osName);

        // Ensure cache directory exists
        try {
            Files.createDirectories(fireboltJdbcDirectory);
        } catch (IOException e) {
            log.error("Failed to create the cached directory.");
        }

        return fireboltJdbcDirectory;
    }

    /**
     * Returns the default directory name for an operating system
     * @param osName - the name of the operating system
     * @return
     */
    @SuppressWarnings("java:S5443") // we are writing to temp directory but the file name is encrypted and so is the content
    private Path getDefaultDirectoryPath(String osName) {
        if (osName.contains("win")) {
            return Paths.get(System.getProperty(TEMP_DIRECTORY_PROPERTY), FIREBOLT_DRIVER_DIRECTORY_NAME);
        } else if (osName.contains("mac")) {
            // this is per user and will be deleted upon reboot or when the OS needs more disk space
            String tempDirectory = System.getenv("TMPDIR");
            return Path.of(tempDirectory, FIREBOLT_DRIVER_DIRECTORY_NAME);
        } else {
            // for linux try the user specific temp directory. If that is not set then use the /tmp
            String tempUserDirectory = System.getenv("XDG_RUNTIME_DIR");
            if (StringUtils.isBlank(tempUserDirectory)) {
                tempUserDirectory = Path.of("/tmp", System.getProperty(USER_HOME_PROPERTY)).toString(); // Fallback if not set
            }
            return Path.of(tempUserDirectory, FIREBOLT_DRIVER_DIRECTORY_NAME);
        }
    }

}
