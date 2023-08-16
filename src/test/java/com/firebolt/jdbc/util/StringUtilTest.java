package com.firebolt.jdbc.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class StringUtilTest {
    @ParameterizedTest
    @CsvSource(value = {
            ",",
            "'',##",
            "a,a",
            "'s,s", "e',e", "'c',c",
            "''s,s", "e'',e", "''c'',c",
            "hello,hello", "'abc,abc", "xyz',xyz", "'something',something",
            "''abc,abc", "xyz'',xyz", "''something'',something"
    },
            delimiter = ',',
            quoteCharacter = '#')
    void trim(String s, String result) {
        assertEquals(result, StringUtil.trim(s, '\''));
    }
}
