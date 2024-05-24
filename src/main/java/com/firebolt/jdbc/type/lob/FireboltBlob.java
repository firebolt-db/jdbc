package com.firebolt.jdbc.type.lob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FireboltBlob implements Blob {
    private byte[] buf;

    public FireboltBlob() {
        this(new byte[0]);
    }

    public FireboltBlob(byte[] buf) {
        this.buf = buf;
    }

    @Override
    public long length() throws SQLException {
        return buf.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        isValid();
        int from = (int)pos - 1;
        if (from < 0 || from > length()) {
            throw new SQLException("Invalid position in Clob object set");
        }
        if (length < 0 || from + length > length()) {
            throw new SQLException("Invalid position and substring length");
        }
        byte[] bytes = new byte[length];
        System.arraycopy(buf, from, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        isValid();
        return new ByteArrayInputStream(buf);
    }

    @Override
    @SuppressWarnings("StatementWithEmptyBody") // so what?
    public long position(byte[] pattern, long start) throws SQLException {
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
    public long position(Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(1, (int)(pattern.length())), start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        isValid();
        if (offset < 0 || offset + len > bytes.length) {
            throw new SQLException("Invalid offset in byte array set");
        }
        if (pos < 1) {
            throw new SQLException("Invalid position in Clob object set");
        }
        int index = (int)(pos - 1);
        int newLength = Math.max(buf.length, index + len);
        byte[] buffer = new byte[newLength];
        System.arraycopy(buf, 0, buffer, 0, buf.length);
        System.arraycopy(bytes, offset, buffer, index, len);
        buf = buffer;
        return len;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        isValid();
        return new OutputStream() {
            private final List<Byte> bytes = new LinkedList<>();
            @Override
            public void write(int b) {
                bytes.add((byte)b);
            }
            public void close() {
                int length = bytes.size();
                int newLength = Math.max(buf.length, length + (int)pos - 1);
                if (newLength > buf.length) {
                    byte[] newBuf = new byte[newLength];
                    System.arraycopy(buf, 0, newBuf, 0, buf.length);
                    buf = newBuf;
                }
                int i = (int)(pos - 1);
                for (byte b : bytes) {
                    buf[i++] = b;
                }
            }
        };
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid();
        buf = length == 0 ? new byte[0] : getBytes(1, (int)length);
    }

    @Override
    public void free() throws SQLException {
        isValid();
        buf = null;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int)length));
    }

    private void isValid() throws SQLException {
        if (buf == null) {
            throw new SQLException("Error: You cannot call a method on a Blob instance once free() has been called.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof FireboltBlob && Arrays.equals(buf, ((FireboltBlob)obj).buf));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buf);
    }
}
