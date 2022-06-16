package io.firebolt.jdbc.exception;

import java.sql.SQLException;

public class FireboltException extends SQLException {

  public FireboltException(String message) {
    super(message);
  }

  public FireboltException(String message, Throwable cause) {
    super(message, cause);
  }
}
