package com.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.RequestBody;

public interface AuthenticationRequest {
	RequestBody getRequestBody() throws JsonProcessingException;

	String getUri();
}