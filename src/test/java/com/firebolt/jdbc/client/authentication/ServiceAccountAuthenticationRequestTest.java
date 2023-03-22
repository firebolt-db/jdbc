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
			"he-ll-o",	"secret", "dev");
		RequestBody requestBody = serviceAccountAuthenticationHttpRequest.getRequestBody();
		Buffer buffer = new Buffer();
		requestBody.writeTo(buffer);

		assertEquals("audience=https%3A%2F%2Fdev-firebolt-v2.us.auth0.com%2Fapi%2Fv2%2F&grant_type=client_credentials&client_id=he-ll-o&client_secret=secret", buffer.readUtf8());
	}

}