package com.firebolt.jdbc.exception;

/**
 * This class will be split to support different types of ExceptionTypes (eg:
 * IO, Conversion, etc)
 */
public enum ExceptionType {
	ERROR, UNAUTHORIZED, TYPE_NOT_SUPPORTED, TYPE_TRANSFORMATION_ERROR, RESOURCE_NOT_FOUND, CANCELED,
	INVALID_REQUEST, TOO_MANY_REQUESTS
}