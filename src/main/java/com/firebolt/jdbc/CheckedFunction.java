package com.firebolt.jdbc;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedFunction<T, R> {
	R apply(T t) throws SQLException;
}
