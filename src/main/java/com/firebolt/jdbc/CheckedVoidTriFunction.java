package com.firebolt.jdbc;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedVoidTriFunction<T, U, V> {
    void apply(T t, U u, V v) throws SQLException;
}
