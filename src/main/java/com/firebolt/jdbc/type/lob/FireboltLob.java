package com.firebolt.jdbc.type.lob;

import java.sql.SQLException;

/**
 * Base class for implementations of Blob and Clob.
 * Unfortunately this cannot be "real" generic base class because Blob and Clob operate with primitive arrays
 * of byte and char that cannot be generalized. Transforming primitive arrays to arrays of corresponding wrappers
 * (Byte and Character) is not used here because of the performance penalty.
 * However, this class contains some utility methods that help to implement minimal code reuse.
 */
abstract class FireboltLob {
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
}
