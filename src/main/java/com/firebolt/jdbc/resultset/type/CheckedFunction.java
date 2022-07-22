package com.firebolt.jdbc.resultset.type;


import java.sql.SQLException;

@FunctionalInterface
public interface CheckedFunction<T, R> {
  R apply(T t) throws SQLException;
}
