package com.firebolt.jdbc.connection;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Generates a new id for each connection. It will be a singleton since we only need one instance for all the connections.
 */
@SuppressWarnings("java:S6548") // suppress the warning for singleton. Yes this is a singleton
public class ConnectionIdGenerator {

    private static ConnectionIdGenerator instance;

    public static synchronized ConnectionIdGenerator getInstance() {
        if (instance == null) {
            instance = new ConnectionIdGenerator();
        }
        return instance;
    }

    /**
     * Will generate a new id. Randomly generate twelve chars (numbers and letters) string as a connection id
     * @return
     */
    public String generateId() {
        return RandomStringUtils.secure().nextAlphanumeric(12);
    }
}
