package com.firebolt.jdbc;

import java.util.Arrays;
import java.util.Optional;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * What type of backend is driver connecting to
 */
@Getter
public enum FireboltBackendType {

    /**
     * firebolt hosted cloud version 2.0
     */
    CLOUD_2_0("cloud"),

    /**
     * only used for testing by packdb
     */
    LOCALHOST("localhost"),

    /**
     * Connecting to firebolt core
     */
    FIREBOLT_CORE("core");

    /**
     * This would be the value that would be present on the connection string
     */
    private String value;

    FireboltBackendType(String value) {
        this.value = value;
    }

    public static Optional<FireboltBackendType> fromString(String value) {
        if (StringUtils.isBlank(value)) {
            return Optional.empty();
        }

        return Arrays.stream(FireboltBackendType.values())
                .filter(fireboltBackendType -> fireboltBackendType.getValue().equalsIgnoreCase(value))
                .findFirst();
    }

}
