package com.firebolt.jdbc.type.lob;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Base class for implementations of Blob and Clob. It operates with primitive arrays (byte[] for Blob and char[] for Clob)
 * and is optimized for better performance using lower level APIs like {@link System#arraycopy(Object, int, Object, int, int)}
 * and minimizes primitive-to-wrapper and back conversions. This is the reason that this class has 2 generic parameters:
 * the array of primitives {@code T} and the element type {@link E}.
 */
abstract class FireboltLob<T, E> {
    private final BiFunction<T, Integer, E> elementGetter;
    private final Function<Integer, E> castor;
    private final Function<Integer, T> bufferFactory;
    protected T buf;

    protected FireboltLob(T buf, BiFunction<T, Integer, E> elementGetter, Function<Integer, E> castor, Function<Integer, T> bufferFactory) {
        this.buf = buf;
        this.elementGetter = elementGetter;
        this.castor = castor;
        this.bufferFactory = bufferFactory;
    }


    protected void validateGetRange(long pos, int length, int bufferLength) throws SQLException {
        int from = (int)(pos - 1);
        if (from < 0 || from > bufferLength) {
            throw new SQLException("Invalid position in Clob object set");
        }
        if (length < 0 || from + length > bufferLength) {
            throw new SQLException("Invalid position and substring length");
        }
    }

    protected void isValid(Object buf) throws SQLException {
        if (buf == null) {
            throw new SQLException("Error: You cannot call a method on a Blob instance once free() has been called.");
        }
    }

    protected void validateSetRange(long pos, int fragmentLength, int offset, int len) throws SQLException {
        if (offset < 0 || offset + len > fragmentLength) {
            throw new SQLException("Invalid offset in byte array set");
        }
        if (pos < 1) {
            throw new SQLException("Invalid position in Clob object set");
        }
    }

    public long length() throws SQLException {
        isValid(buf);
        return Array.getLength(buf);
    }

    @SuppressWarnings("SuspiciousSystemArraycopy") // guaranteed by subclass
    protected int setData(long pos, T data, int offset, int len) throws SQLException {
        isValid(buf);
        validateSetRange(pos, Array.getLength(data), offset, len);
        int index = (int)(pos - 1);
        int bufLength = Array.getLength(buf);
        int newLength = Math.max(bufLength, index + len);
        @SuppressWarnings("unchecked")
        T buffer = (T)Array.newInstance(buf.getClass().getComponentType(), newLength);
        System.arraycopy(buf, 0, buffer, 0, bufLength);
        System.arraycopy(data, offset, buffer, index, len);
        buf = buffer;
        return len;
    }

    public void free() throws SQLException {
        isValid(buf);
        buf = null;
    }

    @SuppressWarnings({"StatementWithEmptyBody", "java:S3776", "java:S127"})
    protected long position(T pattern, long start) throws SQLException {
        isValid(buf);
        int bufLength = Array.getLength(buf);
        if (start < 1 || start > bufLength || bufLength == 0) {
            return -1;
        }
        int patternLength = Array.getLength(pattern);
        if (patternLength == 0) {
            return 1;
        }
        int fromIndex = (int)(start - 1L);
        int max = bufLength - patternLength;
        for (int i = fromIndex; i <= max; i++) {
            if (elementGetter.apply(buf, i) != elementGetter.apply(pattern, 0)) {
                for (i++; i < max && elementGetter.apply(buf, i) != elementGetter.apply(pattern, 0); i++);
            }
            if (i <= max) {
                int j = i + 1;
                int end = j + patternLength - 1;
                for (int k = 1; j < end && elementGetter.apply(buf, j) == elementGetter.apply(pattern, k); j++, k++);
                if (j == end) {
                    return i + 1L;
                }
            }
        }
        return -1;
    }

    protected OutputStream setStream(long pos, List<E> temp) throws SQLException {
        isValid(buf);
        return new OutputStream() {
            private int from = (int)(pos - 1);
            private volatile boolean closed = false;

            @Override
            public void write(int b) throws IOException {
                if (closed) {
                    throw new IOException("Stream is closed");
                }
                temp.add(castor.apply(b));
            }

            @Override
            @SuppressWarnings("SuspiciousSystemArraycopy") // guaranteed by subclass
            public void flush() {
                int length = temp.size();
                int bufLength = Array.getLength(buf);
                int newLength = Math.max(bufLength, length + from);
                if (newLength > bufLength) {
                    T newBuf = bufferFactory.apply(newLength);
                    System.arraycopy(buf, 0, newBuf, 0, bufLength);
                    buf = newBuf;
                }
                for (E b : temp) {
                    Array.set(buf, from++, b);
                }
                temp.clear();
            }

            @Override
            public void close() {
                flush();
                closed = true;
            }
        };
    }
}
