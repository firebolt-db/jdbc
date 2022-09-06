package com.firebolt.jdbc.client.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.Test;

class ServiceAccountAuthenticationRequestTest {

	@Test
	void shouldCreateHttpEntityWithTheProvidedCredentials() throws IOException, ParseException {
		ServiceAccountAuthenticationRequest serviceAccountAuthenticationHttpRequest = new ServiceAccountAuthenticationRequest(
				"he-ll-o", "secret", "https://api.dev.firebolt.io:443");
		HttpEntity httpEntity = serviceAccountAuthenticationHttpRequest.getHttpEntity();
		assertTrue(httpEntity.getContentType().contains("application/x-www-form-urlencoded"));
		assertEquals("client_id=he-ll-o&client_secret=secret&grant_type=client_credentials",
				EntityUtils.toString(httpEntity));
	}

	@Test
	void shouldGetUri() {
		ServiceAccountAuthenticationRequest serviceAccountAuthenticationHttpRequest = new ServiceAccountAuthenticationRequest(
				"he-ll-o", "secret", "https://api.dev.firebolt.io:443");
		assertEquals("https://api.dev.firebolt.io:443/auth/v1/token", serviceAccountAuthenticationHttpRequest.getUri());
	}
}