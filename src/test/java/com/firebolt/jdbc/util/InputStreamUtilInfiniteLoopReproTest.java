package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Repro and regression coverage for the infinite loop in {@link InputStreamUtil#readAllBytes(InputStream)}
 * when every {@link InputStream#read()} throws {@link IOException} (e.g. HTTP/2 stream reset CANCEL).
 *
 * <p>On the buggy implementation (PR #216 style catch-and-continue), {@code readAllBytes} never returns.
 * After the fix it must propagate the {@link IOException} immediately.
 */
class InputStreamUtilInfiniteLoopReproTest {

    private static InputStream alwaysThrowingStream() {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("stream was reset: CANCEL");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("stream was reset: CANCEL");
            }
        };
    }

    /**
     * Fixed behavior: IOException must surface immediately (no hang).
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void readAllBytesPropagatesIoExceptionInsteadOfHanging() {
        assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                assertThrows(IOException.class, () -> InputStreamUtil.readAllBytes(alwaysThrowingStream())));
    }

    /**
     * Captures evidence that the pre-fix loop spins in {@code readAllBytes}.
     * Enabled only when {@code -Drepro.inputstream.hang=true} so the default suite stays green after the fix.
     */
    @Test
    void captureHangStackWhenReproFlagSet() throws InterruptedException {
        // Env var (preferred under Gradle) or -Drepro.inputstream.hang=true on the test JVM.
        boolean captureHang = Boolean.parseBoolean(System.getenv().getOrDefault("REPRO_INPUTSTREAM_HANG", "false"))
                || Boolean.getBoolean("repro.inputstream.hang");
        if (!captureHang) {
            return;
        }

        AtomicReference<StackTraceElement[]> stackDuringHang = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                InputStreamUtil.readAllBytes(alwaysThrowingStream());
            } catch (IOException expectedOnFixedCode) {
                // Fixed code exits here; buggy code never reaches this catch.
            }
        }, "inputstreamutil-hang-repro");
        worker.setDaemon(true);
        worker.start();

        Thread.sleep(2_000);
        if (!worker.isAlive()) {
            fail("readAllBytes returned within 2s — hang is not present (already fixed?). "
                    + "Re-run this flag against pre-fix code to capture the infinite-loop stack.");
        }
        stackDuringHang.set(worker.getStackTrace());

        String stack = Arrays.stream(stackDuringHang.get())
                .map(StackTraceElement::toString)
                .collect(Collectors.joining("\n"));
        System.out.println("=== Stage 1 hang stack (thread still in readAllBytes) ===\n" + stack);
        assertTrue(stack.contains("InputStreamUtil.readAllBytes"),
                "expected stack to show InputStreamUtil.readAllBytes, got:\n" + stack);

        // Demonstrate assertTimeoutPreemptively also fails (times out) on buggy code.
        AssertionError timeout = assertThrows(AssertionError.class, () ->
                assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                    try {
                        InputStreamUtil.readAllBytes(alwaysThrowingStream());
                    } catch (IOException e) {
                        // Fixed code would complete; buggy code never throws out of the loop.
                    }
                }));
        System.out.println("=== Stage 1 assertTimeoutPreemptively failure ===\n" + timeout.getMessage());
        assertFalse(timeout.getMessage() == null || timeout.getMessage().isEmpty());

        // Worker cannot be cleanly interrupted (loop ignores interrupt); leave as daemon.
        fail("Hang repro completed successfully — this failure is intentional so Gradle prints the evidence");
    }
}
