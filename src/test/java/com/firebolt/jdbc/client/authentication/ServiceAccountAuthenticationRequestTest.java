package com.firebolt.jdbc.client.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import okhttp3.RequestBody;
import okio.Buffer;

class ServiceAccountAuthenticationRequestTest {

	@Test
	void shouldCreateHttpEntityWithTheProvidedCredentials() throws IOException {
		ServiceAccountAuthenticationRequest serviceAccountAuthenticationHttpRequest = new ServiceAccountAuthenticationRequest(
				"he-ll-o", "secret", "https://api.dev.firebolt.io:443");
		RequestBody requestBody = serviceAccountAuthenticationHttpRequest.getRequestBody();
		Buffer buffer = new Buffer();
		requestBody.writeTo(buffer);

		assertEquals("client_id=he-ll-o&client_secret=secret&grant_type=client_credentials", buffer.readUtf8());
	}

	@Test
	void shouldGetUri() {
		ServiceAccountAuthenticationRequest serviceAccountAuthenticationHttpRequest = new ServiceAccountAuthenticationRequest(
				"he-ll-o", "secret", "https://api.dev.firebolt.io:443");
		assertEquals("https://api.dev.firebolt.io:443/auth/v1/token", serviceAccountAuthenticationHttpRequest.getUri());
	}
}