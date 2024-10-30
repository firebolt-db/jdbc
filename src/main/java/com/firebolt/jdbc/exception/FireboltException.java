package com.firebolt.jdbc.exception;

import static com.firebolt.jdbc.exception.ExceptionType.*;
import static java.net.HttpURLConnection.*;

import java.sql.SQLException;

import lombok.Getter;

public class FireboltException extends SQLException {

	private static final int HTTP_TOO_MANY_REQUESTS = 429;
	@Getter
	private final ExceptionType type;
	@Getter
	private final String errorMessageFromServer;

	public FireboltException(ExceptionType type) {
		super();
		this.type = type;
		errorMessageFromServer = null;
	}

	public FireboltException(String message) {
		super(message);
		type = ExceptionType.ERROR;
		errorMessageFromServer = null;
	}

	public FireboltException(String message, Integer httpStatusCode) {
		super(message);
		type = getExceptionType(httpStatusCode);
		errorMessageFromServer = null;
	}

	public FireboltException(String message, Integer httpStatusCode, String errorMessageFromServer) {
		super(message);
		type = getExceptionType(httpStatusCode);
		this.errorMessageFromServer = errorMessageFromServer;
	}

	public FireboltException(String message, Integer httpStatusCode, String errorMessageFromServer, SQLState state) {
		super(message, state.getCode());
		type = getExceptionType(httpStatusCode);
		this.errorMessageFromServer = errorMessageFromServer;
	}

	public FireboltException(String message, Throwable cause) {
		this(message, cause, ExceptionType.ERROR);
	}

	public FireboltException(String message, Throwable cause, SQLState state) {
		this(message, cause, ExceptionType.ERROR, state);
	}

	public FireboltException(String message, ExceptionType type) {
		super(message);
		this.type = type;
		errorMessageFromServer = null;
	}

	public FireboltException(String message, Integer httpStatusCode, Throwable cause) {
		this(message, cause, getExceptionType(httpStatusCode));
	}

	public FireboltException(String message, Throwable cause, ExceptionType type) {
		super(message, cause);
		this.type = type;
		errorMessageFromServer = null;
	}

	public FireboltException(String message, Throwable cause, ExceptionType type, SQLState state) {
		super(message, state.getCode(), cause);
		this.type = type;
		errorMessageFromServer = null;
	}

	public FireboltException(String message, int httpStatusCode, SQLState state) {
		super(message, state.getCode());
		type = getExceptionType(httpStatusCode);
		errorMessageFromServer = null;
	}

	private static ExceptionType getExceptionType(Integer httpStatusCode) {
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
		case HTTP_TOO_MANY_REQUESTS:
			return TOO_MANY_REQUESTS;
		default:
			return ERROR;
		}
	}
}
