package com.firebolt.jdbc.util;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StringUtilTest {
    @ParameterizedTest
    @CsvSource(value = {
            ",",
            "``,``",
            "'',``",
            "1,1",
            "true,true",
            "'hello',hello"
    },
    delimiter = ',',
    quoteCharacter = '`')
    void stripSingleQuotes(String value, String expected) {
        assertEquals(expected, StringUtil.strip(value, '\''));
    }

    @Test
    void splitNull() {
        splitAllByTab(null, new String[0]);
    }

    @Test
    void splitEmpty() {
        splitAllByTab("", new String[] { "" });
    }

    @Test
    void splitSingle() {
        splitAllByTab("hello", new String[] {"hello"});
    }

    @Test
    void splitTwo() {
        splitAllByTab("hello\tbye", new String[] {"hello", "bye"});
    }

    @Test
    void splitTwoNullTerminated() {
        splitAllByTab("hello\t", new String[] {"hello", ""});
    }

    @Test
    void splitTwoNullStarted() {
        splitAllByTab("\thello", new String[] {"", "hello"});
    }

    @Test
    void splitConsequentTabs() {
        splitAllByTab("prefix\t\tsuffix", new String[] {"prefix", "", "suffix"});
    }

    private void splitAllByTab(String str, String[] expected) {
        assertArrayEquals(expected, StringUtil.splitAll(str, '\t'));
    }
}