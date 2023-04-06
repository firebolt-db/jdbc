package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class InputStreamUtilTest {

    @Test
    void shouldReadAllBytes() throws IOException {
        String initialString = "text";
        InputStream targetStream = new ByteArrayInputStream(initialString.getBytes());
        InputStreamUtil.readAllBytes(targetStream);
        assertEquals(-1, targetStream.read());
    }

    @Test
    void shouldNotThrowExceptionIfStreamIsNull() {
        assertDoesNotThrow(() -> InputStreamUtil.readAllBytes(null));
    }
}