package com.firebolt.jdbc.exception;

import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static com.firebolt.jdbc.exception.ExceptionType.CANCELED;
import static com.firebolt.jdbc.exception.ExceptionType.CONFLICT;
import static com.firebolt.jdbc.exception.ExceptionType.ERROR;
import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;
import static com.firebolt.jdbc.exception.ExceptionType.REQUEST_BODY_TOO_LARGE;
import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static com.firebolt.jdbc.exception.ExceptionType.TOO_MANY_REQUESTS;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_ENTITY_TOO_LARGE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class FireboltExceptionTest {

    @Test
    void shouldCreateExceptionWithTypeOnly() {
        FireboltException exception = new FireboltException(UNAUTHORIZED);
        
        assertEquals(UNAUTHORIZED, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertNull(exception.getMessage());
    }

    @Test
    void shouldCreateExceptionWithMessage() {
        String message = "Test error message";
        FireboltException exception = new FireboltException(message);
        
        assertEquals(ERROR, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldCreateExceptionWithMessageAndExceptionType() {
        String message = "Test error message";
        FireboltException exception = new FireboltException(message, TOO_MANY_REQUESTS);
        
        assertEquals(TOO_MANY_REQUESTS, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldCreateExceptionWithMessageAndHttpStatusCode() {
        String message = "Test error message";
        FireboltException exception = new FireboltException(message, HTTP_NOT_FOUND);
        
        assertEquals(RESOURCE_NOT_FOUND, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldCreateExceptionWithMessageHttpStatusCodeAndServerError() {
        String message = "Test error message";
        String serverError = "Server internal error";
        FireboltException exception = new FireboltException(message, HTTP_UNAUTHORIZED, serverError);
        
        assertEquals(UNAUTHORIZED, exception.getType());
        assertEquals(serverError, exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldCreateExceptionWithMessageHttpStatusCodeServerErrorAndSQLState() {
        String message = "Test error message";
        String serverError = "Server internal error";
        FireboltException exception = new FireboltException(message, HTTP_BAD_REQUEST, serverError, SQLState.SQL_SYNTAX_ERROR);
        
        assertEquals(INVALID_REQUEST, exception.getType());
        assertEquals(serverError, exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(SQLState.SQL_SYNTAX_ERROR.getCode(), exception.getSQLState());
    }

    @Test
    void shouldCreateExceptionWithMessageAndCause() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, cause);
        
        assertEquals(ERROR, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void shouldCreateExceptionWithMessageCauseAndSQLState() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, cause, SQLState.CONNECTION_EXCEPTION);
        
        assertEquals(ERROR, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(SQLState.CONNECTION_EXCEPTION.getCode(), exception.getSQLState());
    }

    @Test
    void shouldCreateExceptionWithMessageHttpStatusCodeAndCause() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, HTTP_ENTITY_TOO_LARGE, cause);
        
        assertEquals(REQUEST_BODY_TOO_LARGE, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void shouldCreateExceptionWhenHttpStatusIsConflict() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, HTTP_CONFLICT, cause);

        assertEquals(CONFLICT, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void shouldCreateExceptionWithMessageCauseAndExceptionType() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, cause, TYPE_NOT_SUPPORTED);
        
        assertEquals(TYPE_NOT_SUPPORTED, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void shouldCreateExceptionWithMessageCauseExceptionTypeAndSQLState() {
        String message = "Test error message";
        Exception cause = new RuntimeException("Cause exception");
        FireboltException exception = new FireboltException(message, cause, CANCELED, SQLState.QUERY_CANCELED);
        
        assertEquals(CANCELED, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(SQLState.QUERY_CANCELED.getCode(), exception.getSQLState());
    }

    @Test
    void shouldCreateExceptionWithMessageHttpStatusCodeAndSQLState() {
        String message = "Test error message";
        FireboltException exception = new FireboltException(message, HTTP_UNAUTHORIZED, SQLState.INVALID_AUTHORIZATION_SPECIFICATION);
        
        assertEquals(UNAUTHORIZED, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertEquals(SQLState.INVALID_AUTHORIZATION_SPECIFICATION.getCode(), exception.getSQLState());
    }

    /**
     * Test HTTP status code to ExceptionType mapping
     */
    protected static Stream<Arguments> httpStatusCodeToExceptionType() {
        return Stream.of(
                Arguments.of(HTTP_BAD_REQUEST, INVALID_REQUEST),                // 400
                Arguments.of(HTTP_UNAUTHORIZED, UNAUTHORIZED),                  // 401
                Arguments.of(HTTP_NOT_FOUND, RESOURCE_NOT_FOUND),              // 404
                Arguments.of(HTTP_ENTITY_TOO_LARGE, REQUEST_BODY_TOO_LARGE),   // 413
                Arguments.of(429, TOO_MANY_REQUESTS),                          // 429 (HTTP_TOO_MANY_REQUESTS)
                Arguments.of(500, ERROR),                                      // Any other status code
                Arguments.of(999, ERROR),                                      // Unknown status code
                Arguments.of(null, ERROR)                                      // Null status code
        );
    }

    @ParameterizedTest
    @MethodSource("httpStatusCodeToExceptionType")
    void shouldMapHttpStatusCodeToCorrectExceptionType(Integer httpStatusCode, ExceptionType expectedType) {
        String message = "Test message";
        FireboltException exception = new FireboltException(message, httpStatusCode);
        
        assertEquals(expectedType, exception.getType());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldInheritFromSQLException() {
        FireboltException exception = new FireboltException("Test message");
        assertInstanceOf(SQLException.class, exception);
    }

    @Test
    void shouldHandleNullMessage() {
        FireboltException exception = new FireboltException((String) null);
        
        assertEquals(ERROR, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertNull(exception.getMessage());
    }

    @Test
    void shouldHandleNullServerErrorMessage() {
        String message = "Test message";
        FireboltException exception = new FireboltException(message, HTTP_BAD_REQUEST, (String) null);
        
        assertEquals(INVALID_REQUEST, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldHandleNullCause() {
        String message = "Test message";
        FireboltException exception = new FireboltException(message, (Throwable) null);
        
        assertEquals(ERROR, exception.getType());
        assertNull(exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void shouldPreserveOriginalSQLExceptionBehavior() {
        String message = "Test SQL error";
        FireboltException fireboltException = new FireboltException(message, 400, SQLState.SQL_SYNTAX_ERROR);
        
        // FireboltException should behave like SQLException
        assertEquals(message, fireboltException.getMessage());
        assertEquals(SQLState.SQL_SYNTAX_ERROR.getCode(), fireboltException.getSQLState());
        assertInstanceOf(SQLException.class, fireboltException);
    }

    @Test
    void shouldHandleEmptyServerErrorMessage() {
        String message = "Test message";
        String emptyServerError = "";
        FireboltException exception = new FireboltException(message, HTTP_BAD_REQUEST, emptyServerError);
        
        assertEquals(INVALID_REQUEST, exception.getType());
        assertEquals(emptyServerError, exception.getErrorMessageFromServer());
        assertEquals(message, exception.getMessage());
    }

    @Test
    void shouldHandleAllExceptionTypes() {
        for (ExceptionType type : ExceptionType.values()) {
            FireboltException exception = new FireboltException("Test for " + type, type);
            assertEquals(type, exception.getType());
            assertEquals("Test for " + type, exception.getMessage());
        }
    }

    @Test
    void shouldHandleValidSQLState() {
        String message = "Test message";
        FireboltException exception = new FireboltException(message, new RuntimeException(), ERROR, SQLState.SUCCESS);
        
        assertEquals(ERROR, exception.getType());
        assertEquals(message, exception.getMessage());
        assertEquals(SQLState.SUCCESS.getCode(), exception.getSQLState());
    }
    
}
