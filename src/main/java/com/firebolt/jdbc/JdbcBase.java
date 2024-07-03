package com.firebolt.jdbc;

import java.sql.SQLWarning;

public abstract class JdbcBase implements GenericWrapper {
    private SQLWarning firstWarning;

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
