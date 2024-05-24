package com.firebolt.jdbc.type.lob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FireboltClob implements NClob {
    private char[] buf;

    public FireboltClob() {
        this(new char[0]);
    }

    public FireboltClob(char[] buf) {
        this.buf = buf;
    }

    @Override
    public long length() throws SQLException {
        isValid();
        return buf.length;
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        isValid();
        int from = (int)(pos - 1);
        if (from < 0 || from > length()) {
            throw new SQLException("Invalid position in Clob object set");
        }
        if (length < 0 || from + length > length()) {
            throw new SQLException("Invalid position and substring length");
        }
        return new String(buf, from, Math.min(buf.length - from, length));
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        isValid();
        return new StringReader(new String(buf));
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        isValid();
        return new ByteArrayInputStream(new String(buf).getBytes());
    }

    @Override
    public long position(String searchStr, long start) throws SQLException {
        return position(searchStr.toCharArray(), start);
    }

    private long position(char[] pattern, long start) throws SQLException {
        isValid();
        if (start < 1 || start > buf.length || buf.length == 0) {
            return -1;
        }
        if (pattern.length == 0) {
            return 1;
        }

        int fromIndex = (int)(start - 1L);
        int max = buf.length - pattern.length;
        for (int i = fromIndex; i <= max; i++) {
            if (buf[i] != pattern[0]) {
                for (i++; i < max && buf[i] != pattern[0]; i++);
            }
            if (i <= max) {
                int j = i + 1;
                int end = j + pattern.length - 1;
                for (int k = 1; j < end && buf[j] == pattern[k]; j++, k++);
                if (j == end) {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    @Override
    public long position(Clob searchStr, long start) throws SQLException {
        return position(searchStr.getSubString(1, (int)searchStr.length()), start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return setBytes(pos, str.toCharArray(), offset, len);
    }

    private int setBytes(long pos, char[] bytes, int offset, int len) throws SQLException {
        isValid();
        if (offset < 0 || offset + len > bytes.length) {
            throw new SQLException("Invalid offset in byte array set");
        }
        if (pos < 1) {
            throw new SQLException("Invalid position in Clob object set");
        }
        int index = (int)(pos - 1);
        int newLength = Math.max(buf.length, index + len);
        char[] buffer = new char[newLength];
        System.arraycopy(buf, 0, buffer, 0, buf.length);
        System.arraycopy(bytes, offset, buffer, index, len);
        buf = buffer;
        return len;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        isValid();
        return new OutputStream() {
            private final List<Character> characters = new LinkedList<>();
            @Override
            public void write(int b) {
                characters.add((char)b);
            }
            public void close() {
                int length = characters.size();
                int newLength = Math.max(buf.length, length + (int)pos - 1);
                if (newLength > buf.length) {
                    char[] newBuf = new char[newLength];
                    System.arraycopy(buf, 0, newBuf, 0, buf.length);
                    buf = newBuf;
                }
                int i = (int)(pos - 1);
                for (char b : characters) {
                    buf[i++] = b;
                }
            }
        };
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return new OutputStreamWriter(setAsciiStream(pos));
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid();
        buf = length == 0 ? new char[0] : getSubString(1, (int)length).toCharArray();
    }

    @Override
    public void free() throws SQLException {
        isValid();
        buf = null;
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int)length));
    }

    private void isValid() throws SQLException {
        if (buf == null) {
            throw new SQLException("Error: You cannot call a method on a Clob instance once free() has been called.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof FireboltClob && Arrays.equals(buf, ((FireboltClob)obj).buf));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buf);
    }
}
