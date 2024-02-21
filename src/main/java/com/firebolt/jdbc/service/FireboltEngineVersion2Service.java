package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;

import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class FireboltEngineVersion2Service implements FireboltEngineService {
    private final FireboltConnection fireboltConnection;

    public FireboltEngineVersion2Service(FireboltConnection fireboltConnection) {
        this.fireboltConnection = fireboltConnection;
    }

    @Override
    @SuppressWarnings("java:S2077") // Formatting SQL queries is security-sensitive - looks safe in this case
    public Engine getEngine(FireboltProperties properties) throws SQLException {
        try (Statement statement = fireboltConnection.createStatement()) {
            if (properties.getDatabase() != null) {
                statement.executeUpdate(use("DATABASE", properties.getDatabase()));
            }
            statement.executeUpdate(use("ENGINE", properties.getEngine()));
        }
        // now session properties are updated with new database and engine
        FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
        return new Engine(fireboltConnection.getEndpoint(), null, sessionProperties.getEngine(), sessionProperties.getDatabase(), null);
    }

    private String use(String entity, String name) {
        return format("USE %s %s", entity, name);
    }
}
