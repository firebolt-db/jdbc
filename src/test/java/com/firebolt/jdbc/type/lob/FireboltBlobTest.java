package com.firebolt.jdbc.type.lob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FireboltBlobTest {
    @Test
    void empty() throws SQLException, IOException {
        Blob blob = new FireboltBlob();
        assertEquals(0, blob.length());

        assertEquals(0, blob.getBytes(1, 0).length);
        assertThrows(SQLException.class, () -> blob.getBytes(1, 1));
        assertThrows(SQLException.class, () -> blob.getBytes(1, 123));

        assertEquals(-1, blob.getBinaryStream(1, 0).read());
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, 1));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, 123));

        assertEquals(-1, blob.getBinaryStream().read());
        assertArrayEquals(new byte[0], blob.getBinaryStream().readAllBytes());
    }

    @Test
    void preInitialized() throws SQLException, IOException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob(str.getBytes());

        assertEquals(str.length(), blob.length());

        assertEquals("", new String(blob.getBytes(1, 0)));
        assertEquals("h", new String(blob.getBytes(1, 1)));
        assertEquals(str, new String(blob.getBytes(1, str.length())));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, str.length() + 1));

        assertEquals(str, new String(blob.getBinaryStream().readAllBytes()));

        assertEquals(str, new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void binaryStreamToEmptyBlob() throws SQLException, IOException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob();
        try (OutputStream os = blob.setBinaryStream(1)) {
            os.write(str.getBytes());
        }
        assertEquals(str, new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void binaryStreamReplace() throws SQLException, IOException {
        Blob blob = new FireboltBlob("hey, world!".getBytes());
        try (OutputStream os = blob.setBinaryStream(1)) {
            os.write("bye".getBytes());
        }
        assertEquals("bye, world!", new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void binaryStreamReplaceLongerContent() throws SQLException, IOException {
        Blob blob = new FireboltBlob("hello, all!".getBytes());
        try (OutputStream os = blob.setBinaryStream(8)) {
            os.write("world!".getBytes());
        }
        assertEquals("hello, world!", new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void setStringToEmptyBlob() throws SQLException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob();
        blob.setBytes(1, str.getBytes());
        assertEquals(str, new String(blob.getBytes(1, str.length())));
    }

    @Test
    void setStringReplaceCharacters() throws SQLException {
        String str = "hey, world!";
        Blob blob = new FireboltBlob(str.getBytes());
        assertEquals(str, new String(blob.getBytes(1, str.length())));
        blob.setBytes(1, "bye".getBytes());
        assertEquals("bye, world!", new String(blob.getBytes(1, str.length())));
    }

    @Test
    void setStringReplaceCharactersLongerString() throws SQLException {
        String str1 = "hello, all!";
        Blob blob = new FireboltBlob(str1.getBytes());
        assertEquals(str1, new String(blob.getBytes(1, str1.length())));
        blob.setBytes(8, "world!".getBytes());
        String str2 = "hello, world!";
        assertEquals(str2, new String(blob.getBytes(1, str2.length())));
    }

    @Test
    void truncate() throws SQLException, IOException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob(str.getBytes());
        blob.truncate(5);
        assertEquals("hello", new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void truncateLargeNumber() {
        String str = "hello, world!";
        Blob blob = new FireboltBlob(str.getBytes());
        assertThrows(SQLException.class, () -> blob.truncate(str.length() + 1));
    }

    @Test
    void invalid() throws SQLException, IOException {
        Blob blob = new FireboltBlob("".getBytes());
        assertEquals("", new String(blob.getBinaryStream().readAllBytes()));
        blob.free();
        assertThrows(SQLException.class, blob::getBinaryStream);
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
        Blob blob = new FireboltBlob(str.getBytes());
        assertEquals(expected, blob.position(search.getBytes(), start));
        assertEquals(expected, blob.position(new FireboltBlob(search.getBytes()), start));
    }

    @Test
    void equalsAndHashCode() {
        Blob blob1_1 = new FireboltBlob("one".getBytes());
        Blob blob1_2 = new FireboltBlob("one".getBytes());
        assertEquals(blob1_1, blob1_2);
        assertEquals(blob1_1.hashCode(), blob1_2.hashCode());

        Blob blob2_1 = new FireboltBlob("two".getBytes());
        assertNotEquals(blob1_1, blob2_1);
        assertNotEquals(blob1_1.hashCode(), blob2_1.hashCode());
    }
}