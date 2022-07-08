package io.firebolt.jdbc.exception;

public class FireboltUnsupportedOperationException extends UnsupportedOperationException {

  public static final String FEATURE_NOT_SUPPORTED = "JDBC Feature not supported. Method: %s";


  public FireboltUnsupportedOperationException() {
    super(
        String.format(
                FEATURE_NOT_SUPPORTED, Thread.currentThread().getStackTrace()[2].getMethodName()));
  }
}
