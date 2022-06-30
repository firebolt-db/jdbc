package io.firebolt.jdbc.exception;

import lombok.Getter;

import java.sql.SQLException;

public class FireboltException extends SQLException {

  @Getter
  private final ExceptionType type;

  public FireboltException(ExceptionType type) {
    super();
    this.type = type;
  }

  public FireboltException(String message) {
    super(message);
    type = ExceptionType.ERROR;
  }

  public FireboltException(String message, Throwable cause) {
    this(message, cause, ExceptionType.ERROR);
  }

  public FireboltException(String message, ExceptionType type) {
    super(message);
    this.type = type;
  }

  public FireboltException(String message, Throwable cause, ExceptionType type) {
    super(message, cause);
    this.type = type;
  }

}
