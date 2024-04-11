package com.firebolt.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Wrapper;

public class JdbcBase implements Wrapper {
    private SQLWarning firstWarning;

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public synchronized SQLWarning getWarnings() {
        return firstWarning;
    }

    public synchronized void clearWarnings() {
        firstWarning = null;
    }

    protected synchronized void addWarning(SQLWarning sqlWarning) {
        if (firstWarning == null) {
            firstWarning = sqlWarning;
        } else {
            firstWarning.setNextWarning(sqlWarning);
        }
    }
}
