package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression coverage for the former infinite loop in {@link InputStreamUtil#readAllBytes(InputStream)}
 * when every {@link InputStream#read()} throws {@link IOException} (e.g. HTTP/2 stream reset CANCEL).
 *
 * <p>After the fix, {@code readAllBytes} must propagate the {@link IOException} immediately.
 */
class InputStreamUtilInfiniteLoopReproTest {

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void readAllBytesPropagatesIoExceptionInsteadOfHanging() {
        InputStream failing = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("stream was reset: CANCEL");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("stream was reset: CANCEL");
            }
        };
        assertThrows(IOException.class, () -> InputStreamUtil.readAllBytes(failing));
    }
}
