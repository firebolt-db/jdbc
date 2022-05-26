package io.firebolt.jdbc.exception;

import java.sql.SQLException;

public class FireboltException extends SQLException {

  public FireboltException(String errorMessage, Throwable err) {
    super(errorMessage, err);
  }

  public FireboltException(String errorMessage) {
    super(errorMessage);
  }
}
