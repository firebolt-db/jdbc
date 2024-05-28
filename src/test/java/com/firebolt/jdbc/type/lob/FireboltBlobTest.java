package com.firebolt.jdbc.type.lob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FireboltBlobTest extends FireboltLobTest {
    @Test
    void empty() throws SQLException, IOException {
        Blob blob = new FireboltBlob();
        assertEquals(0, blob.length());

        assertEquals(0, blob.getBytes(1, 0).length);
        assertThrows(SQLException.class, () -> blob.getBytes(1, 1));
        assertThrows(SQLException.class, () -> blob.getBytes(1, 123));
        assertThrows(SQLException.class, () -> blob.getBytes(0, 123));
        assertThrows(SQLException.class, () -> blob.getBytes(0, 1));
        assertThrows(SQLException.class, () -> blob.getBytes(1, -1));

        assertEquals(-1, blob.getBinaryStream(1, 0).read());
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, 1));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, 123));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, -1));

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
        assertEquals("hello", new String(blob.getBytes(1, 5)));
        assertEquals("world", new String(blob.getBytes(8, 5)));
        assertEquals("world!", new String(blob.getBytes(8, 6)));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(1, str.length() + 1));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(0, str.length()));
        assertThrows(SQLException.class, () -> blob.getBinaryStream(0, str.length() + 1));

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
    void characterStreamWithFlush() throws SQLException, IOException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob(str.getBytes());
        try (OutputStream os = blob.setBinaryStream(8)) {
            os.write("all".getBytes());
            assertEquals(str, new String(blob.getBinaryStream().readAllBytes()));
            os.flush();
            assertEquals("hello, allld!", new String(blob.getBinaryStream().readAllBytes()));
            os.write(" ".getBytes());
            os.write("people".getBytes());
            os.write("!".getBytes());
            assertEquals("hello, allld!", new String(blob.getBinaryStream().readAllBytes()));
        }
        // the rest is flushed automatically when writer is closed
        assertEquals("hello, all people!", new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void failedToWriteToClosedWriter() throws SQLException, IOException {
        Blob blob = new FireboltBlob();
        OutputStream os = blob.setBinaryStream(1);
        os.close();
        assertThrows(IOException.class, () -> os.write(1));
    }

    @ParameterizedTest
    @MethodSource("replace")
    void binaryStreamReplace(String initial, String replacement, int pos, String expected) throws SQLException, IOException {
        Blob blob = new FireboltBlob(initial.getBytes());
        try (OutputStream os = blob.setBinaryStream(pos)) {
            os.write(replacement.getBytes());
        }
        assertEquals(expected, new String(blob.getBinaryStream().readAllBytes()));
    }

    @Test
    void setStringToEmptyBlob() throws SQLException {
        String str = "hello, world!";
        Blob blob = new FireboltBlob();
        blob.setBytes(1, str.getBytes());
        assertEquals(str, new String(blob.getBytes(1, str.length())));
    }

    @ParameterizedTest
    @MethodSource("replace")
    void stringReplace(String initial, String replacement, int pos, String expected) throws SQLException {
        Blob blob = new FireboltBlob(initial.getBytes());
        assertEquals(initial, new String(blob.getBytes(1, initial.length())));
        blob.setBytes(pos, replacement.getBytes());
        assertEquals(expected, new String(blob.getBytes(1, expected.length())));
    }

    @ParameterizedTest
    @MethodSource("partialReplace")
    void partialStringReplace(String initial, String replacement, int pos, int offset, int length, String expected) throws SQLException {
        Blob blob = new FireboltBlob(initial.getBytes());
        assertEquals(initial, new String(blob.getBytes(1, initial.length())));
        blob.setBytes(pos, replacement.getBytes(), offset, length);
        assertEquals(expected, new String(blob.getBytes(1, expected.length())));
    }

    @ParameterizedTest
    @MethodSource("wrongReplace")
    void wrongReplace(String initial, String replacement, int pos, int offset, int length) throws SQLException {
        Blob blob = new FireboltBlob(initial.getBytes());
        assertEquals(initial, new String(blob.getBytes(1, initial.length())));
        assertThrows(SQLException.class, () -> blob.setBytes(pos, replacement.getBytes(), offset, length));
    }

    @ParameterizedTest
    @MethodSource("truncate")
    void truncate(String str, int length, String expected) throws SQLException, IOException {
        Blob blob = new FireboltBlob(str.getBytes());
        blob.truncate(length);
        assertEquals(expected, new String(blob.getBinaryStream().readAllBytes()));
    }

    @ParameterizedTest
    @ValueSource(longs = {14L, -1L})
    void truncateWrongNumber(long length) {
        String str = "hello, world!";
        Blob blob = new FireboltBlob(str.getBytes());
        assertThrows(SQLException.class, () -> blob.truncate(length));
    }

    @Test
    void invalid() throws SQLException, IOException {
        Blob blob = new FireboltBlob("".getBytes());
        assertEquals("", new String(blob.getBinaryStream().readAllBytes()));
        blob.free();
        assertThrows(SQLException.class, blob::getBinaryStream);
    }

    @ParameterizedTest
    @MethodSource("position")
    void position(String str, String search, int start, int expected) throws SQLException {
        Blob blob = new FireboltBlob(str.getBytes());
        assertEquals(expected, blob.position(search.getBytes(), start));
        assertEquals(expected, blob.position(new FireboltBlob(search.getBytes()), start));
    }

    @ParameterizedTest
    @MethodSource("equalsAndHashCode")
    void equalsAndHashCode(String s1, String s2, boolean expectedEquals) {
        Blob blob1 = new FireboltBlob(s1.getBytes());
        Blob blob2 = new FireboltBlob(s2.getBytes());
        if (expectedEquals) {
            assertEquals(blob1, blob2);
            assertEquals(blob1.hashCode(), blob2.hashCode());
        } else {
            assertNotEquals(blob1, blob2);
            assertNotEquals(blob1.hashCode(), blob2.hashCode());
        }
    }
}