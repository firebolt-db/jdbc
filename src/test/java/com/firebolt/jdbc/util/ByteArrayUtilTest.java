package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigInteger;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteArrayUtilTest {
    @Test
    void stringToByteArray() {
        String str = "hello world";
        String hex = new BigInteger(1, str.getBytes()).toString(16);
        byte[] bytes = ByteArrayUtil.hexStringToByteArray(hex);
        assertEquals(str, new String(bytes));
    }

    @ParameterizedTest
    @CsvSource({"20,' '", "30,0", "2A,*", "2F,/"})
    void hexStringToByteArray(String hex, String expected) {
        assertEquals(expected, new String(ByteArrayUtil.hexStringToByteArray(hex)));
    }

    @ParameterizedTest
    @CsvSource({"2G,G", "H0,H"})
    void wrongHexStringToByteArray(String hex, String expectedWrongCharacter) {
        @SuppressWarnings("java:S5778") // "Refactor the code of the lambda to have only one invocation possibly throwing a runtime exception" - this is the purpose of this test
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new String(ByteArrayUtil.hexStringToByteArray(hex)));
        assertEquals(format("Illegal character %s in hex string", expectedWrongCharacter), e.getMessage());
    }
}
