package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltStatementService;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltCoreConnectionTest {

    private static final String VALID_URL_WITH_DB = "jdbc:firebolt:my_db?";
    private static final String VALID_URL_WITHOUT_DB = "jdbc:firebolt:?";

    @Mock
    private Statement mockStatement;

    @Mock
    private FireboltStatementService fireboltStatementService;

    @ParameterizedTest(name = "Valid URL: {0}")
    @ValueSource(strings = {
            "http://localhost:8080",
            "https://example.com:443",
            "http://192.168.1.1:8080",
            "https://[2001:db8::1]:8080",
            "http://sub.example.com:8080",
            "http://cluster.local:8080",
            "https://cluster.local:8080",
            "http://valid.hostname.:8080",
            "http://valid..hostname:8080",
            "http://.valid.hostname:8080",
            "http://host_name:8080",
            "http://-hostname:8080",
            "http://hostname-:8080",
            "http://256.256.256.256:8080",
            "http://270.0.0.1:8080",
            "http://0.270.0.1:8080",
            "http://0.0.270.1:8080",
            "http://0.0.0.270:8080",
            "http://127.0.1:8080",
            "http://127.0.0:8080",
            "http://127.0:8080",
            "http://127:8080",
            "http://127.0.0.1.1:8080",
            "http://127.0.0.0.0.1:8080",
            "http://127.0.0.:8080",
            "http://127..0.1:8080",
            "http://127.0.0.01:8080",
            "http://127.abc.0.1:8080",
            "http://127.0.0.0x1:8080",
            "http://300.300.300.300:8080",
            "http://127.0.0.-1:8080",
            "http://127.0.0.+1:8080",

            // Kubernetes-style hostnames
            "http://firebolt-core-svc:8080",
            "http://firebolt-core-svc.namespace-foo:8080",
            "http://firebolt-core-svc.namespace-foo.svc:8080",
            "http://firebolt-core-svc.namespace-foo.svc.cluster:8080",
            "http://firebolt-core-svc.namespace-foo.svc.cluster.local:8080"

    })
    void testValidUrls(String url) throws SQLException {
        when(mockStatement.executeUpdate("USE DATABASE \"my_db\"")).thenReturn(0);
        assertDoesNotThrow(() -> createConnection(url));
    }

    @ParameterizedTest(name = "Invalid URL: {0}")
    @ValueSource(strings = {
            "mydomain.com:8080",
            "http://",
            "https://",
            "http://localhost",
            "http://:8080",
            "http://localhost:",
            "http://localhost:0",
            "http://localhost:70000",
            "http://localhost:-1",
            "invalid://localhost:8080",
            // Hostname longer than 256 characters (should fail) - this is 300+ chars
            "http://very-long-hostname-that-exceeds-the-maximum-dns-limit-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-extra:8080",
            // Hostname with one label longer than 63 characters (should fail)
            "http://short.this-label-is-way-too-long-and-exceeds-the-maximum-allowed-length-of-63-characters-for-a-single-dns-label.com:8080"
    })
    void testInvalidUrls(String url) {
        SQLException exception = assertThrows(SQLException.class, () -> createConnection(url));
        assertTrue(exception.getMessage().contains("Invalid URL format") ||
                exception.getMessage().contains("not valid"));
    }

    @Test
    void testMissingUrl() {
        SQLException exception = assertThrows(SQLException.class, () -> createConnection(null));
        assertTrue(exception.getMessage().contains("Url is required for firebolt core"));
    }

    @Test
    void canConnectToCoreWithoutSpecifyingADb() throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITHOUT_DB);
        jdbcUrlBuilder.append("&url=").append("http://localhost:3473");

        try (FireboltCoreConnection connection = new FireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties())) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertFalse(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertTrue(StringUtils.isBlank(fireboltProperties.getDatabase()));
        }

    }

    @Test
    void canConnectOverHttp() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "http://localhost:3473",
                "database", "my_db"
        );
        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertFalse(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertEquals("my_db", fireboltProperties.getDatabase());
        }
    }

    @Test
    void canConnectOverHttps() throws SQLException {
        when(mockStatement.executeUpdate("USE DATABASE \"my_db\"")).thenReturn(0);

        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertTrue(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertEquals("my_db", fireboltProperties.getDatabase());
        }
    }

    @Test
    void shouldStartTransactionAndCommitWhenSwitchingToAutoCommit() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());

            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            connection.createStatement().execute("SELECT 1");

            connection.setAutoCommit(true);
            assertTrue(connection.getAutoCommit());

            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);

            verify(fireboltStatementService, times(4))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statement = statementCaptor.getAllValues();
            assertEquals("USE DATABASE \"my_db\"", statement.get(0).getSql());
            assertEquals("BEGIN TRANSACTION", statement.get(1).getSql());
            assertEquals("SELECT 1", statement.get(2).getSql());
            assertEquals("COMMIT", statement.get(3).getSql());
        }
    }

    @Test
    void shouldThrowExceptionWhenCommittingOrRollbackWithAutoCommitEnabled() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            assertTrue(connection.getAutoCommit());

            FireboltException commitException = assertThrows(FireboltException.class, connection::commit);
            assertEquals("Cannot commit when auto-commit is enabled", commitException.getMessage());

            FireboltException rollbackException = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("Cannot rollback when auto-commit is enabled", rollbackException.getMessage());
        }
    }

    @Test
    void shouldThrowExceptionWhenCommittingOrRollbackWithoutActiveTransaction() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            connection.setAutoCommit(false);

            FireboltException commitException = assertThrows(FireboltException.class, connection::commit);
            assertEquals("No transaction is currently active", commitException.getMessage());

            FireboltException rollbackException = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("No transaction is currently active", rollbackException.getMessage());
        }
    }

    @Test
    void shouldCommitRollbackAndBeginTransactionSuccessfully() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());

            connection.setAutoCommit(false);

            Statement statement1 = connection.createStatement();
            statement1.execute("SELECT 1");
            connection.commit();

            Statement statement2 = connection.createStatement();
            statement2.execute("SELECT 2");
            connection.rollback();

            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);

            verify(fireboltStatementService, times(7))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statement = statementCaptor.getAllValues();
            assertEquals("USE DATABASE \"my_db\"", statement.get(0).getSql());
            assertEquals("BEGIN TRANSACTION", statement.get(1).getSql());
            assertEquals("SELECT 1", statement.get(2).getSql());
            assertEquals("COMMIT", statement.get(3).getSql());
            assertEquals("BEGIN TRANSACTION", statement.get(4).getSql());
            assertEquals("SELECT 2", statement.get(5).getSql());
            assertEquals("ROLLBACK", statement.get(6).getSql());
        }
    }

    @Test
    void shouldHandleTransactionErrorsGracefully() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            connection.setAutoCommit(false);

            doThrow(new SQLException("")).when(fireboltStatementService).execute(any(), any(), any());

            FireboltException exception = assertThrows(FireboltException.class,
                    connection::ensureTransactionForQueryExecution);
            assertEquals("Could not start transaction for query execution", exception.getMessage());
        }
    }

    @Test
    void shouldHandleCommitErrorsGracefully() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            connection.setAutoCommit(false);

            connection.ensureTransactionForQueryExecution();

            doThrow(new SQLException("Commit failed")).when(fireboltStatementService).execute(any(), any(), any());

            FireboltException exception = assertThrows(FireboltException.class, connection::commit);
            assertEquals("Could not commit the transaction", exception.getMessage());
        }
    }

    @Test
    void shouldHandleRollbackErrorsGracefully() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            connection.setAutoCommit(false);

            connection.ensureTransactionForQueryExecution();

            doThrow(new SQLException("Rollback failed")).when(fireboltStatementService).execute(any(), any(), any());

            FireboltException exception = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("Could not rollback the transaction", exception.getMessage());
        }
    }

    @Test
    void shouldRollbackTransactionWhenClosingConnectionWithActiveTransaction() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());

            // Start a transaction
            connection.setAutoCommit(false);
            connection.ensureTransactionForQueryExecution();

            // Verify we're in a transaction
            assertFalse(connection.getAutoCommit());

            // Close the connection - this should trigger a rollback
            connection.close();

            // Verify rollback was called
            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);
            verify(fireboltStatementService, times(3))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statements = statementCaptor.getAllValues();
            //needs to validate db
            assertEquals("USE DATABASE \"my_db\"", statements.get(0).getSql());
            assertEquals("BEGIN TRANSACTION", statements.get(1).getSql());
            assertEquals("ROLLBACK", statements.get(2).getSql());
        }
    }

    @Test
    void shouldNotRollbackWhenClosingConnectionWithoutActiveTransaction() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParamsAndMockStatementService(connectionParams)) {
            // Don't start a transaction - keep auto-commit enabled
            assertTrue(connection.getAutoCommit());

            // Close the connection - this should NOT trigger a rollback
            connection.close();

            // Verify only the USE DATABASE call was made, no ROLLBACK
            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);
            verify(fireboltStatementService, times(1))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statements = statementCaptor.getAllValues();
            assertEquals("USE DATABASE \"my_db\"", statements.get(0).getSql());
        }
    }

    private FireboltCoreConnection createConnection(String url) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITH_DB);

        if (url != null) {
            jdbcUrlBuilder.append("&url=").append(url);
        }

        return aFireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }

    private FireboltCoreConnection createConnectionWithParams(Map<String, String> parameters) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITH_DB);

        String params = parameters.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));

        jdbcUrlBuilder.append("&").append(params);
        return aFireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }

    private FireboltCoreConnection createConnectionWithParamsAndMockStatementService(Map<String, String> parameters) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITH_DB);

        String params = parameters.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));

        jdbcUrlBuilder.append("&").append(params);
        return aFireboltCoreConnectionAndMockStatementService(jdbcUrlBuilder.toString(), new Properties());
    }

    private FireboltCoreConnection aFireboltCoreConnection(String jdbcUrl, Properties properties) throws SQLException {
        return new FireboltCoreConnection(jdbcUrl, properties, null, fireboltStatementService){
            @Override
            public Statement createStatement() {
                return mockStatement;
            }
        };
    }

    private FireboltCoreConnection aFireboltCoreConnectionAndMockStatementService(String jdbcUrl, Properties properties) throws SQLException {
        return new FireboltCoreConnection(jdbcUrl, properties, null, fireboltStatementService);
    }
}
