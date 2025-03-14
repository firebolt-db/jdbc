package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FireboltEngineVersion2ServiceTest {

    private static final long ONE_SECOND = 1;

    private static final String MY_ENGINE = "my_engine";
    private static final String YOUR_ENGINE = "your_engine";

    private static final String MY_DATABASE = "my_database";
    private static final String YOUR_DATABASE = "your_database";

    private static final String MY_ENGINE_ENDPOINT = "api.region.firebolt.io";;
    private static final String YOUR_ENGINE_ENDPOINT = "api2.region.firebolt.io";;

    private static final String SYSTEM_ENGINE_URL = "system.engine.url";

    @ParameterizedTest(name = "database={0}")
    @ValueSource(strings = {MY_DATABASE})
    @NullSource
    void getEngine(String database) throws SQLException {
        Properties props = new Properties();
        if (database != null) {
            props.setProperty("database", database);
        }
        props.setProperty("engine", MY_ENGINE);
        FireboltProperties properties = new FireboltProperties(props);
        FireboltConnection connection = mock(FireboltConnection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        if (database != null) {
            when(statement.executeUpdate("USE DATABASE " + database)).thenReturn(1);
        } else {
            verify(statement, never()).executeQuery("USE DATABASE " + database);
        }
        when(statement.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);
        when(connection.getSessionProperties()).thenReturn(properties);
        when(connection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service  service = new FireboltEngineVersion2Service(connection);
        Engine actualEngine = service.getEngine(properties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, database, null), actualEngine);
    }

    @Test
    void canGetDatabaseAndEngineFromCache() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", MY_DATABASE);
        props.setProperty("engine", MY_ENGINE);

        // we cache based on system engine url and the database/engine
        String host = SYSTEM_ENGINE_URL + RandomStringUtils.randomNumeric(5);
        props.setProperty("host", host);

        FireboltProperties properties = new FireboltProperties(props);

        FireboltConnection firstConnection = mock(FireboltConnection.class);
        Statement statementFromFirstConnection = mock(Statement.class);
        when(firstConnection.createStatement()).thenReturn(statementFromFirstConnection);
        when(statementFromFirstConnection.executeUpdate("USE DATABASE " + MY_DATABASE)).thenReturn(1);
        when(statementFromFirstConnection.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);
        when(firstConnection.getSessionProperties()).thenReturn(properties);
        when(firstConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(firstConnection);
        Engine actualEngine = service.getEngine(properties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should not execute any caching
        verify(statementFromFirstConnection).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(statementFromFirstConnection).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        Properties secondProps = new Properties();
        secondProps.setProperty("database", MY_DATABASE);
        secondProps.setProperty("engine", MY_ENGINE);
        secondProps.setProperty("host", host);

        FireboltProperties secondConnectionProperties = new FireboltProperties(secondProps);

        FireboltConnection secondConnection = mock(FireboltConnection.class);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);

        Statement statementFromSecondConnection = mock(Statement.class);
        when(secondConnection.createStatement()).thenReturn(statementFromSecondConnection);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);
        when(secondConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        doNothing().when(secondConnection).addProperty(anyString(), anyString());
        doNothing().when(secondConnection).setEndpoint(anyString());

        service = new FireboltEngineVersion2Service(secondConnection);
        Engine actualEngineFromCache = service.getEngine(secondConnectionProperties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngineFromCache);

        // should not execute any statements
        verify(statementFromSecondConnection, never()).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(statementFromSecondConnection, never()).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // we should have the database and the engine set on the properties
        assertEquals(MY_DATABASE, secondConnection.getSessionProperties().getDatabase());
        assertEquals(MY_ENGINE, secondConnection.getSessionProperties().getEngine());
    }

    @Test
    void canGetDatabaseAndEngineFromSourceWhenCacheExpired() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", MY_DATABASE);
        props.setProperty("engine", MY_ENGINE);

        // we cache based on system engine url and the database/engine
        String host = SYSTEM_ENGINE_URL + RandomStringUtils.randomNumeric(5);
        props.setProperty("host", host);

        FireboltProperties properties = new FireboltProperties(props);

        FireboltConnection firstConnection = mock(FireboltConnection.class);
        Statement statementFromFirstConnection = mock(Statement.class);
        when(firstConnection.createStatement()).thenReturn(statementFromFirstConnection);
        when(statementFromFirstConnection.executeUpdate("USE DATABASE " + MY_DATABASE)).thenReturn(1);
        when(statementFromFirstConnection.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);
        when(firstConnection.getSessionProperties()).thenReturn(properties);
        when(firstConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        // keep the values in cache for only 1 second
        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(firstConnection, ONE_SECOND, ONE_SECOND);
        Engine actualEngine = service.getEngine(properties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        verify(statementFromFirstConnection).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(statementFromFirstConnection).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // sleep for 2 seconds just to make sure the cache expired
        sleepForMillis(TimeUnit.SECONDS.toMillis(2));

        Properties secondProps = new Properties();
        secondProps.setProperty("database", MY_DATABASE);
        secondProps.setProperty("engine", MY_ENGINE);
        secondProps.setProperty("host", host);

        FireboltProperties secondConnectionProperties = new FireboltProperties(secondProps);

        FireboltConnection secondConnection = mock(FireboltConnection.class);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);

        Statement statementFromSecondConnection = mock(Statement.class);
        when(secondConnection.createStatement()).thenReturn(statementFromSecondConnection);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);
        when(secondConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);
        when(statementFromSecondConnection.executeUpdate("USE DATABASE " + MY_DATABASE)).thenReturn(1);
        when(statementFromSecondConnection.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);

        doNothing().when(secondConnection).addProperty(anyString(), anyString());
        doNothing().when(secondConnection).setEndpoint(anyString());

        service = new FireboltEngineVersion2Service(secondConnection);
        Engine actualEngineFromCache = service.getEngine(secondConnectionProperties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngineFromCache);

        // should not execute any caching
        verify(statementFromSecondConnection).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(statementFromSecondConnection).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // we should have the database and the engine set on the properties
        assertEquals(MY_DATABASE, secondConnection.getSessionProperties().getDatabase());
        assertEquals(MY_ENGINE, secondConnection.getSessionProperties().getEngine());
    }

    @Test
    void canGetDatabaseAndEngineFromSourceWhenDifferentDatabaseAndEngineAreCached() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", MY_DATABASE);
        props.setProperty("engine", MY_ENGINE);

        // we cache based on system engine url and the database/engine
        String host = SYSTEM_ENGINE_URL + RandomStringUtils.randomNumeric(5);
        props.setProperty("host", host);

        FireboltProperties properties = new FireboltProperties(props);

        FireboltConnection firstConnection = mock(FireboltConnection.class);
        Statement statementFromFirstConnection = mock(Statement.class);
        when(firstConnection.createStatement()).thenReturn(statementFromFirstConnection);
        when(statementFromFirstConnection.executeUpdate("USE DATABASE " + MY_DATABASE)).thenReturn(1);
        when(statementFromFirstConnection.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);
        when(firstConnection.getSessionProperties()).thenReturn(properties);
        when(firstConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(firstConnection);
        Engine actualEngine = service.getEngine(properties);
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        verify(statementFromFirstConnection).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(statementFromFirstConnection).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // use a diffent database and engine. Values should be obtained from the source

        Properties secondProps = new Properties();
        secondProps.setProperty("database", YOUR_DATABASE);
        secondProps.setProperty("engine", YOUR_ENGINE);
        secondProps.setProperty("host", host);

        FireboltProperties secondConnectionProperties = new FireboltProperties(secondProps);

        FireboltConnection secondConnection = mock(FireboltConnection.class);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);

        Statement statementFromSecondConnection = mock(Statement.class);
        when(secondConnection.createStatement()).thenReturn(statementFromSecondConnection);
        when(secondConnection.getSessionProperties()).thenReturn(secondConnectionProperties);
        when(secondConnection.getEndpoint()).thenReturn(YOUR_ENGINE_ENDPOINT);
        when(statementFromSecondConnection.executeUpdate("USE DATABASE " + MY_DATABASE)).thenReturn(1);
        when(statementFromSecondConnection.executeUpdate("USE ENGINE " + MY_ENGINE)).thenReturn(1);

        service = new FireboltEngineVersion2Service(secondConnection);
        Engine yourEngine = service.getEngine(secondConnectionProperties);
        assertEquals(new Engine(YOUR_ENGINE_ENDPOINT, null, YOUR_ENGINE, YOUR_DATABASE, null), yourEngine);

        verify(statementFromSecondConnection).executeUpdate("USE DATABASE \"" + YOUR_DATABASE + "\"");
        verify(statementFromSecondConnection).executeUpdate("USE ENGINE \"" + YOUR_ENGINE + "\"");
    }

    private void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // do nothing
        }
    }


}