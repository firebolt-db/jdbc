package com.firebolt.jdbc.client.authentication;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

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

		assertEquals(format("audience=%s&grant_type=client_credentials&client_id=he-ll-o&client_secret=secret", URLEncoder.encode("https://api.firebolt.io", StandardCharsets.UTF_8)), buffer.readUtf8());
	}

}