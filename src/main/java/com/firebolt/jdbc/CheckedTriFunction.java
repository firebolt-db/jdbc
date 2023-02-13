package com.firebolt.jdbc;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedTriFunction<T, U, V, R> {
    R apply(T t, U u, V v) throws SQLException;
}
