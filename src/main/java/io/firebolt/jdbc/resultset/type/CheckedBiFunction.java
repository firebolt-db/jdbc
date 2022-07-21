package io.firebolt.jdbc.resultset.type;

import io.firebolt.jdbc.exception.FireboltException;

@FunctionalInterface
public interface CheckedBiFunction<T, U, R> {
  R apply(T t, U u) throws FireboltException;
}