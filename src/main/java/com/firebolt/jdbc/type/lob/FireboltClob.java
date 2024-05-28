package com.firebolt.jdbc.type.lob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Array;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;

public class FireboltClob extends FireboltLob<char[], Character> implements NClob {
    public FireboltClob() {
        this(new char[0]);
    }

    public FireboltClob(char[] buf) {
        super(buf, (a, i) -> a[i], i -> (char)i.intValue(), n -> (char[])Array.newInstance(char.class, n));
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        isValid(buf);
        validateGetRange(pos, length, buf.length);
        int from = (int)(pos - 1);
        return new String(buf, from, Math.min(buf.length - from, length));
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        isValid(buf);
        return new StringReader(new String(buf));
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        isValid(buf);
        return new ByteArrayInputStream(new String(buf).getBytes());
    }

    @Override
    public long position(String searchStr, long start) throws SQLException {
        return position(searchStr.toCharArray(), start);
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
        return setChars(pos, str.toCharArray(), offset, len);
    }

    private int setChars(long pos, char[] chars, int offset, int len) throws SQLException {
        return setData(pos, chars, offset, len);
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return setStream(pos, new LinkedList<>());
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return new OutputStreamWriter(setAsciiStream(pos));
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid(buf);
        buf = length == 0 ? new char[0] : getSubString(1, (int)length).toCharArray();
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int)length));
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
