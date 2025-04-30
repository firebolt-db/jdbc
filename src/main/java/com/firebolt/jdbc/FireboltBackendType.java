package com.firebolt.jdbc;

import lombok.Getter;

/**
 * What type of backend is driver connecting to
 */
@Getter
public enum FireboltBackendType {

    /**
     * firebolt hosted cloud version 1.0
     */
    CLOUD_1_0("cloudv1"),

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

}
