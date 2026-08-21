package com.firebolt.jdbc.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.util.InputStreamUtil;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deterministic HTTP/2 repro: MockWebServer (H2 prior knowledge) accepts a DML POST, begins the
 * response body, then resets the stream with {@link ErrorCode#CANCEL}. On buggy
 * {@link InputStreamUtil#readAllBytes}, {@link FireboltStatement#executeUpdate(String)} hangs and
 * WARN-spams; after the fix it returns a {@link SQLException} promptly.
 *
 * <p>Uses {@link Protocol#H2_PRIOR_KNOWLEDGE} so HTTP/2 duplex / RST_STREAM works without relying on
 * TLS ALPN (which can fall back to HTTP/1.1 on some JDKs).
 */
class Http2StreamResetExecuteUpdateHangTest {

    private MockWebServer mockWebServer;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger inputStreamUtilLogger;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.setProtocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE));
        mockWebServer.start();

        inputStreamUtilLogger = (Logger) LoggerFactory.getLogger(InputStreamUtil.class);
        // Avoid flooding RollingFileAppender during the infinite-loop repro (can crash the test JVM).
        inputStreamUtilLogger.detachAndStopAllAppenders();
        logAppender = new ListAppender<>();
        logAppender.start();
        inputStreamUtilLogger.addAppender(logAppender);
        inputStreamUtilLogger.setLevel(Level.WARN);
        inputStreamUtilLogger.setAdditive(false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (inputStreamUtilLogger != null && logAppender != null) {
            inputStreamUtilLogger.detachAppender(logAppender);
        }
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
        System.out.println("Stage 2 (fixed): executeUpdate failed in " + elapsedMs + "ms: " + thrown);
    }

    /**
     * Hang / WARN-spam evidence for the buggy drain loop. Enable with env
     * {@code REPRO_HTTP2_HANG=true} (or {@code -Drepro.http2.hang=true} on the test JVM).
     */
    @Test
    void captureHangAndWarnSpamWhenReproFlagSet() throws Exception {
        boolean captureHang = Boolean.parseBoolean(System.getenv().getOrDefault("REPRO_HTTP2_HANG", "false"))
                || Boolean.getBoolean("repro.http2.hang");
        if (!captureHang) {
            return;
        }

        enqueueCancelAfterPartialBody();
        FireboltStatement statement = createStatementAgainstMock();

        AtomicReference<StackTraceElement[]> stackDuringHang = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "executeUpdate-http2-hang-repro");
            t.setDaemon(true);
            return t;
        });

        int warnBefore = countWarnEvents();
        Future<?> future = executor.submit(() -> {
            started.countDown();
            statement.executeUpdate("INSERT INTO jdbc_repro_drop_me VALUES (1)");
            return null;
        });

        assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread.sleep(500);
        if (future.isDone()) {
            try {
                future.get();
                System.out.println("=== Stage 2 DEBUG: executeUpdate returned normally ===");
            } catch (Exception e) {
                System.out.println("=== Stage 2 DEBUG: executeUpdate completed with error ===");
                e.printStackTrace(System.out);
            }
        }
        assertTrue(!future.isDone(), "expected executeUpdate to still be hanging on buggy code");

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if ("executeUpdate-http2-hang-repro".equals(t.getName())) {
                stackDuringHang.set(t.getStackTrace());
                break;
            }
        }
        assertNotNull(stackDuringHang.get(), "could not find watchdog thread");
        String stack = Arrays.stream(stackDuringHang.get())
                .map(StackTraceElement::toString)
                .collect(Collectors.joining("\n"));
        System.out.println("=== Stage 2 hang stack ===\n" + stack);
        assertTrue(stack.contains("InputStreamUtil.readAllBytes")
                        || stack.contains("readAllBytes"),
                "expected stack in readAllBytes, got:\n" + stack);

        int warnAtStart = countWarnEvents();
        Thread.sleep(200);
        int warnAtEnd = countWarnEvents();
        int warnDelta = warnAtEnd - warnAtStart;
        double warnsPerSecond = warnDelta / 0.2;
        System.out.println("=== Stage 2 WARN spam === warns in 200ms window=" + warnDelta
                + " (~" + String.format("%.0f", warnsPerSecond) + "/s), totalCaptured=" + warnAtEnd
                + " (baseline before call=" + warnBefore + ")");
        assertTrue(warnDelta > 10, "expected high-frequency WARN spam, got " + warnDelta);

        // Stop further log allocation from the still-looping daemon thread.
        inputStreamUtilLogger.setLevel(Level.OFF);
        executor.shutdownNow();
        fail("HTTP/2 hang repro completed — intentional failure so evidence is printed");
    }

    private int countWarnEvents() {
        // Snapshot under lock: the hung drain thread keeps appending concurrently.
        List<ILoggingEvent> snapshot;
        synchronized (logAppender.list) {
            snapshot = new java.util.ArrayList<>(logAppender.list);
        }
        return (int) snapshot.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .filter(e -> e.getFormattedMessage() != null
                        && e.getFormattedMessage().contains("Could not read entire input stream"))
                .count();
    }

    private void enqueueCancelAfterPartialBody() {
        // Custom duplex: write a partial body (no END_STREAM), then RST_STREAM CANCEL so the
        // client's ResponseBody InputStream throws StreamResetException on further reads.
        mockWebServer.enqueue(new MockResponse()
                .clearHeaders()
                .setBody((DuplexResponseBody) (request, stream) -> {
                    okio.BufferedSink sink = okio.Okio.buffer(stream.getSink());
                    sink.writeUtf8("partial-response-body");
                    sink.flush();
                    // Do not close the sink with END_STREAM — reset instead.
                    stream.close(ErrorCode.CANCEL, null);
                }));
    }

    private FireboltStatement createStatementAgainstMock() throws SQLException {
        OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
                .build();

        FireboltConnection connection = mock(FireboltConnection.class);
        // ssl=false → http:// scheme matching cleartext H2 prior knowledge
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

        StatementClientImpl statementClient = new StatementClientImpl(client, connection, "", "");
        FireboltStatementService service = new FireboltStatementService(statementClient);
        return new FireboltStatement(service, properties, connection);
    }
}
