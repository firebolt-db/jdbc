package com.firebolt.jdbc.client.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.RequestBody;
import okio.Buffer;

class UsernamePasswordAuthenticationRequestTest {

	@Test
	void shouldCreateHttpEntityWithTheProvidedCredentials() throws IOException {
		UsernamePasswordAuthenticationRequest usernamePasswordAuthenticationHttpRequest = new UsernamePasswordAuthenticationRequest(
				"hello", "pa$$word", "https://api.dev.firebolt.io:443");
		RequestBody requestBody = usernamePasswordAuthenticationHttpRequest.getRequestBody();
		Buffer buffer = new Buffer();
		requestBody.writeTo(buffer);
		// We transform the requests to map because the order of the fields is not
		// guaranteed
		Map<String, Object> expectedRequest = new ObjectMapper()
				.readValue("{\"username\":\"hello\",\"password\":\"pa$$word\"}", HashMap.class);
		Map<String, Object> actualRequest = new ObjectMapper().readValue(buffer.readUtf8(), HashMap.class);
		assertEquals(expectedRequest, actualRequest);
	}

	@Test
	void getUri() {
		UsernamePasswordAuthenticationRequest usernamePasswordAuthenticationHttpRequest = new UsernamePasswordAuthenticationRequest(
				"hello", "pa$$word", "https://api.dev.firebolt.io:443");
		assertEquals("https://api.dev.firebolt.io:443/auth/v1/login",
				usernamePasswordAuthenticationHttpRequest.getUri());
	}
}