package com.firebolt.jdbc.service;

import com.firebolt.jdbc.cache.ConnectionCache;
import com.firebolt.jdbc.cache.DatabaseOptions;
import com.firebolt.jdbc.cache.EngineOptions;
import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltEngineVersion2ServiceTest {

    private static final String MY_ENGINE = "my_engine";

    private static final String MY_DATABASE = "my_database";

    private static final String MY_ENGINE_ENDPOINT = "api.region.firebolt.io";

    @Mock
    private ConnectionCache mockConnectionCache;
    @Mock
    private FireboltConnection mockFireboltConnection;
    @Mock
    private Statement mockFireboltStatement;

    @Captor
    private ArgumentCaptor<DatabaseOptions> databaseOptionsArgumentCaptor;
    @Captor
    private ArgumentCaptor<EngineOptions> engineOptionsArgumentCaptor;

    private FireboltProperties properties;
    private FireboltProperties sessionProperties;

    @BeforeEach
    void initMethod() {
        Properties props = new Properties();
        props.setProperty("database", MY_DATABASE);
        props.setProperty("engine", MY_ENGINE);

        properties = new FireboltProperties(props);

        sessionProperties = new FireboltProperties(new Properties());
        when(mockFireboltConnection.getSessionProperties()).thenReturn(sessionProperties);
    }

    @ParameterizedTest(name = "database={0}")
    @ValueSource(strings = {MY_DATABASE})
    @NullSource
    void getEngine(String database) throws SQLException {
        Properties props = new Properties();
        if (database != null) {
            props.setProperty("database", database);
        }
        props.setProperty("engine", MY_ENGINE);
        properties = new FireboltProperties(props);
        Statement statement = mock(Statement.class);
        when(mockFireboltConnection.createStatement()).thenReturn(statement);
        if (database != null) {
            when(statement.executeUpdate("USE DATABASE \"" + database + "\"")).thenReturn(1);
        } else {
            verify(statement, never()).executeQuery("USE DATABASE \"" + database + "\"");
        }
        when(statement.executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"")).thenReturn(1);
        when(mockFireboltConnection.getSessionProperties()).thenReturn(properties);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service  service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.empty());
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, database, null), actualEngine);
    }

    @Test
    void canGetDatabaseAndEngineFromSourceWhenCachingIsNotEnabled() throws SQLException {
        when(mockFireboltConnection.createStatement()).thenReturn(mockFireboltStatement);
        when(mockFireboltStatement.executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"")).thenReturn(1);
        when(mockFireboltStatement.executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"")).thenReturn(1);
        when(mockFireboltConnection.getSessionProperties()).thenReturn(properties);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.empty());
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should make calls to the system engine url to check the database and engine
        verify(mockFireboltStatement).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(mockFireboltStatement).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");
    }

    @Test
    void canGetDatabaseAndEngineFromSourceWhenCachingIsEnabledButDatabaseNotInCache() throws SQLException {
        when(mockConnectionCache.getDatabaseOptions(MY_DATABASE)).thenReturn(Optional.empty());
        when(mockConnectionCache.getEngineOptions(MY_ENGINE)).thenReturn(Optional.empty());

        doNothing().when(mockConnectionCache).setDatabaseOptions(eq(MY_DATABASE), any());
        doNothing().when(mockConnectionCache).setEngineOptions(eq(MY_ENGINE), any());

        // as a side effect of executing the "USE DATABASE xxx" the session properties will have the database name populated
        sessionProperties.addProperty("database", MY_DATABASE);
        sessionProperties.addProperty("engine", MY_ENGINE);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        when(mockFireboltConnection.createStatement()).thenReturn(mockFireboltStatement);
        when(mockFireboltStatement.executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"")).thenReturn(1);
        when(mockFireboltStatement.executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"")).thenReturn(1);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.of(mockConnectionCache));
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should make calls to the system engine url to check the database and engine
        verify(mockFireboltStatement).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(mockFireboltStatement).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // should save the values in cache
        verify(mockConnectionCache).setDatabaseOptions(eq(MY_DATABASE), databaseOptionsArgumentCaptor.capture());
        DatabaseOptions savedDatabaseOptions = databaseOptionsArgumentCaptor.getValue();
        List<Pair<String,String>> databaseParams = savedDatabaseOptions.getParameters();
        assertEquals(1, databaseParams.size());
        assertEquals("database", databaseParams.get(0).getKey());
        assertEquals(MY_DATABASE, databaseParams.get(0).getValue());

        verify(mockConnectionCache).setEngineOptions(eq(MY_ENGINE), engineOptionsArgumentCaptor.capture());
        EngineOptions engineOptions = engineOptionsArgumentCaptor.getValue();
        assertEquals(MY_ENGINE_ENDPOINT, engineOptions.getEngineUrl());
        assertEquals(1, engineOptions.getParameters().size());
        assertEquals("engine", engineOptions.getParameters().get(0).getKey());
        assertEquals(MY_ENGINE, engineOptions.getParameters().get(0).getValue());
    }

    @Test
    void canGetDatabaseAndEngineFromCache() throws SQLException {
        when(mockFireboltConnection.createStatement()).thenReturn(mockFireboltStatement);

        DatabaseOptions databaseOptions = new DatabaseOptions(List.of(Pair.of("database", MY_DATABASE)));
        when(mockConnectionCache.getDatabaseOptions(MY_DATABASE)).thenReturn(Optional.of(databaseOptions));

        EngineOptions engineOptions = new EngineOptions(MY_ENGINE_ENDPOINT, List.of(Pair.of("engine", MY_ENGINE)));
        when(mockConnectionCache.getEngineOptions(MY_ENGINE)).thenReturn(Optional.of(engineOptions));

        doNothing().when(mockFireboltConnection).addProperty(anyString(), anyString(), eq(false));

        sessionProperties.addProperty("database", MY_DATABASE);
        sessionProperties.addProperty("engine", MY_ENGINE);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.of(mockConnectionCache));
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should not make db calls
        verify(mockFireboltStatement, never()).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(mockFireboltStatement, never()).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // should not update any values in cache
        verify(mockConnectionCache, never()).setDatabaseOptions(any(), any());
        verify(mockConnectionCache, never()).setEngineOptions(any(), any());

        // check that we should set the cached properties on the connection
        verify(mockFireboltConnection).addProperty("database", MY_DATABASE, false);
        verify(mockFireboltConnection).addProperty("engine", MY_ENGINE, false);
        verify(mockFireboltConnection).setEndpoint(MY_ENGINE_ENDPOINT);
    }

    @Test
    void willOnlyGetDatabaseFromSourceIfEngineIsAlreadyCached() throws SQLException {
        when(mockFireboltConnection.createStatement()).thenReturn(mockFireboltStatement);

        // database not cached
        when(mockConnectionCache.getDatabaseOptions(MY_DATABASE)).thenReturn(Optional.empty());

        // but engine cached
        EngineOptions engineOptions = new EngineOptions(MY_ENGINE_ENDPOINT, List.of(Pair.of("engine", MY_ENGINE)));
        when(mockConnectionCache.getEngineOptions(MY_ENGINE)).thenReturn(Optional.of(engineOptions));

        doNothing().when(mockFireboltConnection).addProperty(anyString(), anyString(), eq(false));

        sessionProperties.addProperty("database", MY_DATABASE);
        sessionProperties.addProperty("engine", MY_ENGINE);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.of(mockConnectionCache));
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should only make the call to get the database
        verify(mockFireboltStatement).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(mockFireboltStatement, never()).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // should update the database options
        verify(mockConnectionCache).setDatabaseOptions(eq(MY_DATABASE), databaseOptionsArgumentCaptor.capture());
        verify(mockConnectionCache, never()).setEngineOptions(any(), any());

        DatabaseOptions savedDatabaseOptions = databaseOptionsArgumentCaptor.getValue();
        List<Pair<String,String>> databaseParams = savedDatabaseOptions.getParameters();
        assertEquals(1, databaseParams.size());
        assertEquals("database", databaseParams.get(0).getKey());
        assertEquals(MY_DATABASE, databaseParams.get(0).getValue());

        // check that we should set the cached properties on the connection for engine only
        verify(mockFireboltConnection, never()).addProperty("database", MY_DATABASE, false);
        verify(mockFireboltConnection).addProperty("engine", MY_ENGINE, false);
        verify(mockFireboltConnection).setEndpoint(MY_ENGINE_ENDPOINT);
    }

   @Test
    void willOnlyGetEngineFromSourceIfDatabaseIsAlreadyCached() throws SQLException {
        when(mockFireboltConnection.createStatement()).thenReturn(mockFireboltStatement);

        // database cached
       DatabaseOptions databaseOptions = new DatabaseOptions(List.of(Pair.of("database", MY_DATABASE)));
       when(mockConnectionCache.getDatabaseOptions(MY_DATABASE)).thenReturn(Optional.of(databaseOptions));

       // but engine not cached
        when(mockConnectionCache.getEngineOptions(MY_ENGINE)).thenReturn(Optional.empty());

        doNothing().when(mockFireboltConnection).addProperty(anyString(), anyString(), eq(false));

        sessionProperties.addProperty("database", MY_DATABASE);
        sessionProperties.addProperty("engine", MY_ENGINE);
        when(mockFireboltConnection.getEndpoint()).thenReturn(MY_ENGINE_ENDPOINT);

        FireboltEngineVersion2Service service = new FireboltEngineVersion2Service(mockFireboltConnection);
        Engine actualEngine = service.getEngine(properties, Optional.of(mockConnectionCache));
        assertEquals(new Engine(MY_ENGINE_ENDPOINT, null, MY_ENGINE, MY_DATABASE, null), actualEngine);

        // should only make the call to get the engine
        verify(mockFireboltStatement, never()).executeUpdate("USE DATABASE \"" + MY_DATABASE + "\"");
        verify(mockFireboltStatement).executeUpdate("USE ENGINE \"" + MY_ENGINE + "\"");

        // should update the engine options
        verify(mockConnectionCache, never()).setDatabaseOptions(any(), any());
        verify(mockConnectionCache).setEngineOptions(eq(MY_ENGINE), engineOptionsArgumentCaptor.capture());

       verify(mockConnectionCache).setEngineOptions(eq(MY_ENGINE), engineOptionsArgumentCaptor.capture());
       EngineOptions engineOptions = engineOptionsArgumentCaptor.getValue();
       assertEquals(MY_ENGINE_ENDPOINT, engineOptions.getEngineUrl());
       assertEquals(1, engineOptions.getParameters().size());
       assertEquals("engine", engineOptions.getParameters().get(0).getKey());
       assertEquals(MY_ENGINE, engineOptions.getParameters().get(0).getValue());

       // check that we should set the cached properties on the connection for database only
        verify(mockFireboltConnection).addProperty("database", MY_DATABASE, false);
        verify(mockFireboltConnection, never()).addProperty("engine", MY_ENGINE, false);
        verify(mockFireboltConnection, never()).setEndpoint(MY_ENGINE_ENDPOINT);
    }

}