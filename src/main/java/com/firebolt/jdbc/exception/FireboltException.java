package com.firebolt.jdbc.exception;

import lombok.Getter;

import java.sql.SQLException;

import static com.firebolt.jdbc.exception.ExceptionType.*;
import static java.net.HttpURLConnection.*;

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

	public FireboltException(String message, Integer httpStatusCode) {
		super(message);
		type = getExceptionType(httpStatusCode);
	}

	public FireboltException(String message, Throwable cause) {
		this(message, cause, ExceptionType.ERROR);
	}

	public FireboltException(String message, ExceptionType type) {
		super(message);
		this.type = type;
	}

	public FireboltException(String message, Integer httpStatusCode, Throwable cause) {
		super(message, cause);
		this.type = getExceptionType(httpStatusCode);
	}

	public FireboltException(String message, Throwable cause, ExceptionType type) {
		super(message, cause);
		this.type = type;
	}

	private ExceptionType getExceptionType(Integer httpStatusCode) {
		if (httpStatusCode == null) {
			return ERROR;
		}
		switch (httpStatusCode) {
		case HTTP_NOT_FOUND:
			return RESOURCE_NOT_FOUND;
		case HTTP_BAD_REQUEST:
			return INVALID_REQUEST;
		case HTTP_UNAUTHORIZED:
			return UNAUTHORIZED;
		case 429:
			return TOO_MANY_REQUESTS;
		default:
			return ERROR;
		}
	}
}
