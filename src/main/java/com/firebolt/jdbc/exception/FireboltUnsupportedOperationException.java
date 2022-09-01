package com.firebolt.jdbc.exception;

public class FireboltUnsupportedOperationException extends UnsupportedOperationException {

	public static final String OPERATION_NOT_SUPPORTED = "JDBC Operation not supported. Method: %s, Line: %d";

	public FireboltUnsupportedOperationException() {
		super(String.format(OPERATION_NOT_SUPPORTED, Thread.currentThread().getStackTrace()[2].getMethodName(),
				Thread.currentThread().getStackTrace()[2].getLineNumber()));
	}
}
