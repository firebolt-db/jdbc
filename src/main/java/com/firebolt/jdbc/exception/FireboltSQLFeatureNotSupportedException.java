package com.firebolt.jdbc.exception;

import java.sql.SQLFeatureNotSupportedException;

public class FireboltSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

	public static final String FEATURE_NOT_SUPPORTED = "JDBC Feature not supported. Method: %s, Line: %d";

	public FireboltSQLFeatureNotSupportedException() {
		super(String.format(FEATURE_NOT_SUPPORTED, Thread.currentThread().getStackTrace()[2].getMethodName(),
				Thread.currentThread().getStackTrace()[2].getLineNumber()));
	}

	public FireboltSQLFeatureNotSupportedException(String message) {
		super(message);
	}
}
