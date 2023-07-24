package com.firebolt.jdbc.exception;

import java.sql.SQLFeatureNotSupportedException;

@SuppressWarnings("squid:MaximumInheritanceDepth")
public class FireboltUnsupportedOperationException extends SQLFeatureNotSupportedException {

	public static final String OPERATION_NOT_SUPPORTED = "JDBC Operation not supported. Method: %s, Line: %d";

	public FireboltUnsupportedOperationException() {
		super(String.format(OPERATION_NOT_SUPPORTED, Thread.currentThread().getStackTrace()[2].getMethodName(),
				Thread.currentThread().getStackTrace()[2].getLineNumber()));
	}
}
