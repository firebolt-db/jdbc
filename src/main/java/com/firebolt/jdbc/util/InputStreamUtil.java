package com.firebolt.jdbc.util;

import lombok.experimental.UtilityClass;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

@UtilityClass
public class InputStreamUtil {
    private static final int K_BYTE = 1024;
    private static final int BUFFER_SIZE = 8 * K_BYTE;

    /**
     * Read all bytes from the input stream if the stream is not null.
     *
     * @param is input stream
     * @throws IOException if reading the stream fails before EOF
     */
    public void readAllBytes(@Nullable InputStream is) throws IOException {
        if (is != null) {
            // Drain until EOF. Use a buffer to avoid per-byte syscalls on healthy streams.
            byte[] buffer = new byte[BUFFER_SIZE];
            while (is.read(buffer) != -1) {
                // discard
            }
        }
    }

    public String read(Reader initialReader, int limit) throws IOException {
        char[] arr = new char[BUFFER_SIZE];
        StringBuilder buffer = new StringBuilder();
        int numCharsRead;
        while ((numCharsRead = initialReader.read(arr, 0, arr.length)) != -1) {
            buffer.append(arr, 0, numCharsRead);
            if (buffer.length() >= limit) {
                break;
            }
        }
        return buffer.length() > limit ? buffer.substring(0, limit) : buffer.toString();
    }
}
