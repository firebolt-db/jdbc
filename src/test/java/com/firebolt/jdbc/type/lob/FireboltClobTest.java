package com.firebolt.jdbc.type.lob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

class FireboltClobTest {
    @Test
    void empty() throws SQLException, IOException {
        Clob clob = new FireboltClob();
        assertEquals(0, clob.length());

        assertEquals(0, clob.getSubString(1, 0).length());
        assertThrows(SQLException.class, () -> clob.getSubString(1, 1));
        assertThrows(SQLException.class, () -> clob.getSubString(1, 123));

        assertEquals(-1, clob.getCharacterStream(1, 0).read());
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, 1));
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, 123));

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
        assertThrows(SQLException.class, () -> clob.getCharacterStream(1, str.length() + 1));

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

    @Test
    void characterStreamReplace() throws SQLException, IOException {
        Clob clob = new FireboltClob("hey, world!".toCharArray());
        try (Writer writer = clob.setCharacterStream(1)) {
            writer.write("bye");
        }
        assertEquals("bye, world!", readAll(clob.getCharacterStream()));
    }

    @Test
    void characterStreamReplaceLongerContent() throws SQLException, IOException {
        Clob clob = new FireboltClob("hello, all!".toCharArray());
        try (Writer writer = clob.setCharacterStream(8)) {
            writer.write("world!");
        }
        assertEquals("hello, world!", readAll(clob.getCharacterStream()));
    }

    @Test
    void setStringToEmptyClob() throws SQLException {
        String str = "hello, world!";
        Clob clob = new FireboltClob();
        clob.setString(1, str);
        assertEquals(str, clob.getSubString(1, str.length()));
    }

    @Test
    void setStringReplaceCharacters() throws SQLException {
        String str = "hey, world!";
        Clob clob = new FireboltClob(str.toCharArray());
        assertEquals(str, clob.getSubString(1, str.length()));
        clob.setString(1, "bye");
        assertEquals("bye, world!", clob.getSubString(1, str.length()));
    }

    @Test
    void setStringReplaceCharactersLongerString() throws SQLException {
        String str1 = "hello, all!";
        Clob clob = new FireboltClob(str1.toCharArray());
        assertEquals(str1, clob.getSubString(1, str1.length()));
        clob.setString(8, "world!");
        String str2 = "hello, world!";
        assertEquals(str2, clob.getSubString(1, str2.length()));
    }

    @Test
    void truncate() throws SQLException, IOException {
        String str = "hello, world!";
        Clob clob = new FireboltClob(str.toCharArray());
        clob.truncate(5);
        assertEquals("hello", new String(clob.getAsciiStream().readAllBytes()));
    }

    @Test
    void truncateLargeNumber() {
        String str = "hello, world!";
        Clob clob = new FireboltClob(str.toCharArray());
        assertThrows(SQLException.class, () -> clob.truncate(str.length() + 1));
    }

    @Test
    void invalid() throws SQLException, IOException {
        Clob clob = new FireboltClob("".toCharArray());
        assertEquals("", new String(clob.getAsciiStream().readAllBytes()));
        clob.free();
        assertThrows(SQLException.class, clob::getAsciiStream);
    }

    @ParameterizedTest
    @CsvSource({
            "hello,1,1",
            "world,1,8",
            "world,3,8",
            "world,8,8",
            "world,9,-1",
            "world,0,-1",
            "world,14,-1",
    })
    void position(String search, int start, int expected) throws SQLException {
        String str = "hello, world!";
        Clob clob = new FireboltClob(str.toCharArray());
        assertEquals(expected, clob.position(search, start));
        assertEquals(expected, clob.position(new FireboltClob(search.toCharArray()), start));
    }

    @Test
    void equalsAndHashCode() {
        Clob clob1_1 = new FireboltClob("one".toCharArray());
        Clob clob1_2 = new FireboltClob("one".toCharArray());
        assertEquals(clob1_1, clob1_2);
        assertEquals(clob1_1.hashCode(), clob1_2.hashCode());

        Clob clob2_1 = new FireboltClob("two".toCharArray());
        assertNotEquals(clob1_1, clob2_1);
        assertNotEquals(clob1_1.hashCode(), clob2_1.hashCode());
    }

    private String readAll(Reader reader) {
        return new BufferedReader(reader).lines().collect(Collectors.joining(System.lineSeparator()));
    }
}