package com.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static java.net.HttpURLConnection.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationClientTest {
	private static final String HOST = "https://host";
	private static final String USER = "usr";
	private static final String PASSWORD = "PA§§WORD";

	@Spy
	private final ObjectMapper objectMapper = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Captor
	private ArgumentCaptor<Request> requestArgumentCaptor;
	@Mock
	private OkHttpClient httpClient;
	private FireboltAuthenticationClient fireboltAuthenticationClient;

	@Mock
	private FireboltConnection connection;

	@BeforeEach
	void setUp() {
		fireboltAuthenticationClient = new FireboltAuthenticationClient(httpClient, objectMapper, connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
	}

	@Test
	void shouldPostConnectionTokens() throws IOException, FireboltException {
		Response response = mock(Response.class);
		Call call = mock(Call.class);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(response.code()).thenReturn(HTTP_OK);
		when(httpClient.newCall(any())).thenReturn(call);
		when(call.execute()).thenReturn(response);
		String tokensResponse = new ObjectMapper().writeValueAsString(
				FireboltAuthenticationResponse.builder().accessToken("a").refreshToken("r").expiresIn(1).build());
		when(body.string()).thenReturn(tokensResponse);

		fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD);

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		Request actualPost = requestArgumentCaptor.getValue();
		assertEquals("User-Agent", actualPost.headers().iterator().next().getFirst());
		assertEquals(userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), actualPost.headers().iterator().next().getSecond());
		verify(objectMapper).readValue(tokensResponse, FireboltAuthenticationResponse.class);
	}

	@Test
	void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
		Response response = mock(Response.class);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(response.code()).thenReturn(HTTP_NOT_FOUND);
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(FireboltException.class,
				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
	}

	@Test
	void shouldNotRetryWhenFacingANonRetryableException() throws Exception {
		Call call = mock(Call.class);
		when(call.execute()).thenThrow(IOException.class);
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(IOException.class, () -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
		verify(call).execute();
		verify(call, times(0)).clone();
	}

	@Test
	void shouldThrowExceptionWhenStatusCodeIsForbidden() throws Exception {
		Response response = mock(Response.class);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(response.code()).thenReturn(HTTP_FORBIDDEN);
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(FireboltException.class,
				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
	}
}
