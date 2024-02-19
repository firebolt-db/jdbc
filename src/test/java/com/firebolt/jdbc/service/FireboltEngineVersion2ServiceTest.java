package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltEngineVersion2ServiceTest {
    @Test
    void getEndine() throws SQLException {
        String database = "my_database";
        String engine = "my_engine";
        Properties props = new Properties();
        props.setProperty("database", database);
        props.setProperty("engine", engine);
        String endpoint = "api.region.firebolt.io";
        FireboltProperties properties = new FireboltProperties(props);
        FireboltConnection connection = mock(FireboltConnection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeUpdate("USE DATABASE " + database)).thenReturn(1);
        when(statement.executeUpdate("USE ENGINE " + engine)).thenReturn(1);
        when(connection.getSessionProperties()).thenReturn(properties);
        when(connection.getEndpoint()).thenReturn(endpoint);

        FireboltEngineVersion2Service  service = new FireboltEngineVersion2Service(connection);
        Engine actualEngine = service.getEngine(properties);
        assertEquals(new Engine(endpoint, null, engine, database, null), actualEngine);
    }
}