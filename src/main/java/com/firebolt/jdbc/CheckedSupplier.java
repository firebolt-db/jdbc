package com.firebolt.jdbc;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedSupplier<R> {
    R get() throws SQLException;
}