package io.firebolt.jdbc;


import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

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

    public static int getMajorVersion() {
        return Optional.ofNullable(version).map(v -> StringUtils.split(v, ".")[0]).filter(StringUtils::isNumeric)
                .map(Integer::parseInt).orElseThrow(() -> new RuntimeException("Invalid driver version: could not parse major version"));
    }


    public int getMinorVersion() {
        return Optional.ofNullable(version).map(v -> StringUtils.split(v, ".")[1])
                .map(minorVersion -> RegExUtils.replaceAll(minorVersion, "[^0-9]", "")) //Remove all non-numeric characters
                .map(Integer::parseInt).orElseThrow(() -> new RuntimeException("Invalid driver version: could not parse major version"));
    }
    public static String getProjectVersion() {
        return version;
    }
}
