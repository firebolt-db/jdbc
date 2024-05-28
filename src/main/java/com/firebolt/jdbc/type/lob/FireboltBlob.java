package com.firebolt.jdbc.type.lob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;

public class FireboltBlob extends FireboltLob<byte[], Byte> implements Blob {
    public FireboltBlob() {
        this(new byte[0]);
    }

    public FireboltBlob(byte[] buf) {
        super(buf, (a, i) -> a[i], Integer::byteValue, n -> (byte[])Array.newInstance(byte.class, n));
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        isValid(buf);
        validateGetRange(pos, length, buf.length);
        int from = (int)pos - 1;
        byte[] bytes = new byte[length];
        System.arraycopy(buf, from, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        isValid(buf);
        return new ByteArrayInputStream(buf);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return super.position(pattern, start);
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
        return setData(pos, bytes, offset, len);
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return setStream(pos, new LinkedList<>());
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid(buf);
        buf = length == 0 ? new byte[0] : getBytes(1, (int)length);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int)length));
    }

    @Override
    @SuppressWarnings("java:S6201") // Pattern Matching for "instanceof" was introduced in java 16 while we still try to be compliant with java 11
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof FireboltBlob && Arrays.equals(buf, ((FireboltBlob)obj).buf));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buf);
    }
}
