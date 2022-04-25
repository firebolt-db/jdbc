package io.firebolt.jdbc;


import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@UtilityClass
public class ProjectVersionUtil {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ProjectVersionUtil.class);

    private static String version;

    static {
        Properties properties = new Properties();
        version = null;
        try {
            properties.load(ProjectVersionUtil.class.getResourceAsStream("/version.properties"));
            version = properties.getProperty("version");
        } catch (IOException e) {
            log.error("Could not get Project Version defined in the build.gradle file", e);
        }
    }

    public static Optional<String> getProjectVersion() {
        return Optional.ofNullable(version);
    }
}
