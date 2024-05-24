package com.firebolt.jdbc.type.lob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FireboltClobTest extends FireboltLobTest {
    @Test
    void empty() throws SQLException, IOException {
        Clob clob = new FireboltClob();
        assertEquals(0, clob.length());

        assertEquals(0, clob.getSubString(1, 0).length());
        assertThrows(SQLException.class, () -> clob.getSubString(1, 1));
        assertThrows(SQLException.class, () -> clob.getSubString(1, 123));
        assertThrows(SQLException.class, () -> clob.getSubString(0, 123));
        assertThrows(SQLException.class, () -> clob.getSubString(0, 1));
        assertThrows(SQLException.class, () -> clob.getSubString(1, -1));

        assertEquals(-1, clob.getCharacterStream(1, 0).read());
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, 1));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, 123));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, -1));

        assertEquals(-1, clob.getAsciiStream().read());
        assertEquals("", readAll(clob.getCharacterStream()));
    }

    @Test
    void preInitialized() throws SQLException, IOException {
        String str = "hello, world!";
        Clob clob = new FireboltClob(str.toCharArray());

        assertEquals(str.length(), clob.length());

        assertEquals("", clob.getSubString(1, 0));
        assertEquals("h", clob.getSubString(1, 1));
        assertEquals(str, clob.getSubString(1, str.length()));
        assertEquals("hello", clob.getSubString(1, 5));
        assertEquals("world", clob.getSubString(8, 5));
        assertEquals("world!", clob.getSubString(8, 6));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, str.length() + 1));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(0, str.length()));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(0, str.length() + 1));

        assertEquals(str, new String(clob.getAsciiStream().readAllBytes()));

        assertEquals(str, readAll(clob.getCharacterStream()));
    }

    @Test
    void asciiStream() throws SQLException, IOException {
        String str = "hello, world!";
        Clob clob = new FireboltClob();
        try (OutputStream os = clob.setAsciiStream(1)) {
            os.write(str.getBytes());
        }
        assertEquals(str, new String(clob.getAsciiStream().readAllBytes()));
    }

    @Test
    void characterStream() throws SQLException, IOException {
        String str = "hello, world!";
        Clob clob = new FireboltClob();
        try (Writer writer = clob.setCharacterStream(1)) {
            writer.write(str);
        }
        assertEquals(str, readAll(clob.getCharacterStream()));
    }

    @ParameterizedTest
    @MethodSource("replace")
    void binaryStreamReplace(String initial, String replacement, int pos, String expected) throws SQLException, IOException {
        Clob clob = new FireboltClob(initial.toCharArray());
        try (Writer writer = clob.setCharacterStream(pos)) {
            writer.write(replacement);
        }
        assertEquals(expected, readAll(clob.getCharacterStream()));
    }

    @ParameterizedTest
    @MethodSource("replace")
    void stringReplaceCharacters(String initial, String replacement, int pos, String expected) throws SQLException {
        Clob clob = new FireboltClob(initial.toCharArray());
        assertEquals(initial, clob.getSubString(1, initial.length()));
        clob.setString(pos, replacement);
        assertEquals(expected, clob.getSubString(1, expected.length()));
    }

    @ParameterizedTest
    @MethodSource("partialReplace")
    void partialStringReplace(String initial, String replacement, int pos, int offset, int length, String expected) throws SQLException {
        Clob clob = new FireboltClob(initial.toCharArray());
        assertEquals(initial, clob.getSubString(1, initial.length()));
        clob.setString(pos, replacement, offset, length);
        assertEquals(expected, clob.getSubString(1, expected.length()));
    }

    @ParameterizedTest
    @MethodSource("wrongReplace")
    void wrongReplace(String initial, String replacement, int pos, int offset, int length) throws SQLException {
        Clob clob = new FireboltClob(initial.toCharArray());
        assertEquals(initial, clob.getSubString(1, initial.length()));
        assertThrows(SQLException.class, () -> clob.setString(pos, replacement, offset, length));
    }

    @Test
    void setStringToEmptyClob() throws SQLException {
        String str = "hello, world!";
        Clob clob = new FireboltClob();
        clob.setString(1, str);
        assertEquals(str, clob.getSubString(1, str.length()));
    }

    @ParameterizedTest
    @MethodSource("truncate")
    void truncate(String str, int length, String expected) throws SQLException, IOException {
        Clob clob = new FireboltClob(str.toCharArray());
        clob.truncate(length);
        assertEquals(expected, new String(clob.getAsciiStream().readAllBytes()));
    }

    @ParameterizedTest
    @ValueSource(longs = {14L, -1L})
    void truncateWrongNumber(long length) {
        String str = "hello, world!";
        Clob clob = new FireboltClob(str.toCharArray());
        assertThrows(SQLException.class, () -> clob.truncate(length));
    }

    @Test
    void invalid() throws SQLException, IOException {
        Clob clob = new FireboltClob("".toCharArray());
        assertEquals("", new String(clob.getAsciiStream().readAllBytes()));
        clob.free();
        assertThrows(SQLException.class, clob::getAsciiStream);
    }

    @ParameterizedTest
    @MethodSource("position")
    void position(String str, String search, int start, int expected) throws SQLException {
        Clob clob = new FireboltClob(str.toCharArray());
        assertEquals(expected, clob.position(search, start));
        assertEquals(expected, clob.position(new FireboltClob(search.toCharArray()), start));
    }

    @ParameterizedTest
    @MethodSource("equalsAndHashCode")
    void equalsAndHashCode(String s1, String s2, boolean expectedEquals) {
        Clob clob1 = new FireboltClob(s1.toCharArray());
        Clob clob2 = new FireboltClob(s2.toCharArray());
        if (expectedEquals) {
            assertEquals(clob1, clob2);
            assertEquals(clob1.hashCode(), clob2.hashCode());
        } else {
            assertNotEquals(clob1, clob2);
            assertNotEquals(clob1.hashCode(), clob2.hashCode());
        }
    }

    private String readAll(Reader reader) {
        return new BufferedReader(reader).lines().collect(Collectors.joining(System.lineSeparator()));
    }
}