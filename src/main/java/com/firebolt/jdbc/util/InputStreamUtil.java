package com.firebolt.jdbc.util;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

@UtilityClass
@CustomLog
public class InputStreamUtil {

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
}
