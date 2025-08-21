package com.firebolt.jdbc.exception;

/**
 * This class represents the types of exceptions that may be thrown
 */
public enum ExceptionType {
	ERROR, UNAUTHORIZED,
	TYPE_NOT_SUPPORTED,
	TYPE_TRANSFORMATION_ERROR,
	RESOURCE_NOT_FOUND,
	CANCELED,
	INVALID_REQUEST,
	TOO_MANY_REQUESTS,
	REQUEST_BODY_TOO_LARGE
}