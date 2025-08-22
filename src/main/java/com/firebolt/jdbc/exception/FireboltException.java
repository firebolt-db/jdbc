package com.firebolt.jdbc.exception;

import java.sql.SQLException;
import lombok.Getter;

import static com.firebolt.jdbc.exception.ExceptionType.ERROR;
import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;
import static com.firebolt.jdbc.exception.ExceptionType.REQUEST_BODY_TOO_LARGE;
import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static com.firebolt.jdbc.exception.ExceptionType.TOO_MANY_REQUESTS;
import static com.firebolt.jdbc.exception.ExceptionType.UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_ENTITY_TOO_LARGE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

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

		// have them ordered based on the status code
		switch (httpStatusCode) {
			case HTTP_BAD_REQUEST:              // 400
				return INVALID_REQUEST;
			case HTTP_UNAUTHORIZED:             // 401
				return UNAUTHORIZED;
			case HTTP_NOT_FOUND:                // 404
				return RESOURCE_NOT_FOUND;
			case HTTP_ENTITY_TOO_LARGE:         // 413
				return REQUEST_BODY_TOO_LARGE;
			case HTTP_TOO_MANY_REQUESTS:        // 429
				return TOO_MANY_REQUESTS;
			default:
				return ERROR;
		}
	}
}
