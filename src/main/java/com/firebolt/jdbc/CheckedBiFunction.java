package com.firebolt.jdbc;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedBiFunction<T, U, R> {
    R apply(T t, U u) throws SQLException;
}