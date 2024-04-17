package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

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

    @ParameterizedTest
    @CsvSource(value = {
            "hello,5,hello",
            "hello,6,hello",
            "hello,42,hello",
            "hello,4,hell",
            "hello,1,h",
            "hello,0,''",
    })
    void read(String in, int length, String expected) throws IOException {
        assertEquals(expected, InputStreamUtil.read(new StringReader(in), length));
    }
}