package com.firebolt.jdbc.type;

import com.firebolt.jdbc.exception.FireboltException;

@FunctionalInterface
public interface CheckedBiFunction<T, U, R> {
	R apply(T t, U u) throws FireboltException;
}
