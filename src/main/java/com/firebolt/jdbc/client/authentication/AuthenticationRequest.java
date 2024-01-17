package com.firebolt.jdbc.client.authentication;

import okhttp3.RequestBody;

public interface AuthenticationRequest {
	RequestBody getRequestBody();

	String getUri();
}