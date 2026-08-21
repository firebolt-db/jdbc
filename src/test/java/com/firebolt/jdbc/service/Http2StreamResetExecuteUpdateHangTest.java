package com.firebolt.jdbc.service;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.SQLState;
import com.firebolt.jdbc.statement.FireboltStatement;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.EventListener;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.internal.http2.ErrorCode;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.internal.duplex.DuplexResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * HTTP/2 regression coverage for non-query response drain failures.
 *
 * <p>MockWebServer with {@link Protocol#H2_PRIOR_KNOWLEDGE} accepts a DML POST, begins the response
 * body, then resets the stream with {@link ErrorCode#CANCEL}. {@code executeUpdate} must fail fast
 * with SQLState {@code 08007}, and a subsequent statement must succeed on a fresh connection.
 */
class Http2StreamResetExecuteUpdateHangTest {

    private MockWebServer mockWebServer;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.setProtocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE));
        mockWebServer.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (mockWebServer != null) {
            mockWebServer.close();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void executeUpdateFailsFastWhenHttp2StreamResetMidBody() throws Exception {
        enqueueCancelAfterPartialBody();

        FireboltStatement statement = createStatementAgainstMock();
        long started = System.nanoTime();
        SQLException thrown = assertThrows(SQLException.class,
                () -> statement.executeUpdate("INSERT INTO jdbc_repro_drop_me VALUES (1)"));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(elapsedMs < 5_000,
                "expected SQLException within milliseconds/seconds after fix, took " + elapsedMs + "ms");
        assertNotNull(thrown.getMessage());
        assertTrue(thrown.getMessage().toLowerCase().contains("interrupted")
                        || thrown.getMessage().toLowerCase().contains("stream")
                        || thrown.getCause() instanceof IOException,
                "unexpected message: " + thrown.getMessage());
        assertEquals(SQLState.TRANSACTION_RESOLUTION_UNKNOWN.getCode(), thrown.getSQLState());
    }

    /**
     * After a drain failure, the connection pool is evicted and a subsequent statement on the same
     * JDBC statement/service must succeed on a fresh HTTP connection (not the reset one).
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void secondStatementSucceedsOnFreshConnectionAfterDrainFailure() throws Exception {
        List<Connection> acquiredConnections = Collections.synchronizedList(new ArrayList<>());
        EventListener connectionTracker = new EventListener() {
            @Override
            public void connectionAcquired(Call call, Connection connection) {
                acquiredConnections.add(connection);
            }
        };

        enqueueCancelAfterPartialBody();
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(""));

        StatementFixture fixture = createStatementAgainstMock(connectionTracker);
        FireboltStatement statement = fixture.statement;

        SQLException first = assertThrows(SQLException.class,
                () -> statement.executeUpdate("INSERT INTO jdbc_repro_drop_me VALUES (1)"));
        assertEquals(SQLState.TRANSACTION_RESOLUTION_UNKNOWN.getCode(), first.getSQLState());
        verify(fixture.statementClient).evictConnectionPool();

        statement.executeUpdate("INSERT INTO jdbc_repro_drop_me VALUES (2)");

        assertTrue(acquiredConnections.size() >= 2,
                "expected at least two connection acquisitions, got " + acquiredConnections.size());
        assertNotSame(acquiredConnections.get(0), acquiredConnections.get(1),
                "second request must not reuse the first (reset) HTTP connection");
        assertEquals(2, mockWebServer.getRequestCount());
    }

    private void enqueueCancelAfterPartialBody() {
        mockWebServer.enqueue(new MockResponse()
                .clearHeaders()
                .setBody((DuplexResponseBody) (request, stream) -> {
                    okio.BufferedSink sink = okio.Okio.buffer(stream.getSink());
                    sink.writeUtf8("partial-response-body");
                    sink.flush();
                    stream.close(ErrorCode.CANCEL, null);
                }));
    }

    private FireboltStatement createStatementAgainstMock() throws SQLException {
        return createStatementAgainstMock(EventListener.NONE).statement;
    }

    private StatementFixture createStatementAgainstMock(EventListener eventListener) throws SQLException {
        OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
                .eventListener(eventListener)
                .build();

        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltProperties properties = FireboltProperties.builder()
                .host(mockWebServer.getHostName())
                .port(mockWebServer.getPort())
                .database("benchmark_agent_db")
                .compress(false)
                .ssl(false)
                .accessToken("test-token")
                .build();

        when(connection.getAccessToken()).thenReturn(Optional.of("test-token"));
        when(connection.getBackendType()).thenReturn(FireboltBackendType.DEV);
        when(connection.getInfraVersion()).thenReturn(2);
        when(connection.getSessionProperties()).thenReturn(properties);
        lenient().doNothing().when(connection).ensureTransactionForQueryExecution();

        StatementClientImpl statementClient = spy(new StatementClientImpl(client, connection, "", ""));
        FireboltStatementService service = new FireboltStatementService(statementClient);
        FireboltStatement statement = new FireboltStatement(service, properties, connection);
        return new StatementFixture(statement, statementClient);
    }

    private static final class StatementFixture {
        final FireboltStatement statement;
        final StatementClientImpl statementClient;

        StatementFixture(FireboltStatement statement, StatementClientImpl statementClient) {
            this.statement = statement;
            this.statementClient = statementClient;
        }
    }
}
