package com.firebolt.jdbc.util;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

@UtilityClass
@CustomLog
public class InputStreamUtil {
    private static final int K_BYTE = 1024;
    private static final int BUFFER_SIZE = 8 * K_BYTE;

    /**
     * Read all bytes from the input stream if the stream is not null
     *
     * @param is input stream
     */
    public void readAllBytes(@Nullable InputStream is) {
        if (is != null) {
            while (true) {
                try {
                    if (is.read() == -1) break;
                } catch (IOException e) {
                    log.warn("Could not read entire input stream for non query statement", e);
                }
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
