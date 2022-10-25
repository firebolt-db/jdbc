package com.firebolt.jdbc.client.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class UsernamePasswordAuthenticationRequestTest {

	@Test
	void shouldCreateHttpEntityWithTheProvidedCredentials() throws IOException, ParseException {
		UsernamePasswordAuthenticationRequest usernamePasswordAuthenticationHttpRequest = new UsernamePasswordAuthenticationRequest(
				"hello", "pa$$word", "https://api.dev.firebolt.io:443");
		HttpEntity httpEntity = usernamePasswordAuthenticationHttpRequest.getHttpEntity();

		//We transform the requests to map because the order of the fields is not guaranteed
		Map<String,Object> expectedRequest =
				new ObjectMapper().readValue("{\"username\":\"hello\",\"password\":\"pa$$word\"}", HashMap.class);
		Map<String,Object> actualRequest =
				new ObjectMapper().readValue(EntityUtils.toString(httpEntity), HashMap.class);
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