package com.firebolt.jdbc.client.authentication;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationClientTest {
	private static final String HOST = "https://host";
	private static final String USER = "usr";
	private static final String PASSWORD = "PA§§WORD";

	private static final String ENV = "ENV";

	@Captor
	private ArgumentCaptor<Request> requestArgumentCaptor;
	@Mock
	private OkHttpClient httpClient;
	private FireboltAuthenticationClient fireboltAuthenticationClient;

	@Mock
	private FireboltConnection connection;

	@BeforeEach
	void setUp() {
		fireboltAuthenticationClient = new FireboltAuthenticationClient(httpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9") {
			@Override
			protected AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
				AuthenticationRequest request = Mockito.mock(AuthenticationRequest.class);
				when(request.getUri()).thenReturn("http://host/auth");
				return request;
			}
		};
	}

	@Test
	void shouldPostConnectionTokens() throws SQLException, IOException {
		Response response = mock(Response.class);
		Call call = mock(Call.class);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(response.code()).thenReturn(HTTP_OK);
		when(httpClient.newCall(any())).thenReturn(call);
		when(call.execute()).thenReturn(response);
		String tokensResponse = "{\"access_token\":\"a\", \"refresh_token\":\"r\", \"expires_in\":1}";
		when(body.string()).thenReturn(tokensResponse);

		FireboltConnectionTokens tokens = fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD, ENV);
		assertEquals("a", tokens.getAccessToken());
		assertEquals(1, tokens.getExpiresInSeconds());

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		Request actualPost = requestArgumentCaptor.getValue();
		assertEquals("User-Agent", actualPost.headers().iterator().next().getFirst());
		assertEquals(userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), actualPost.headers().iterator().next().getSecond());
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
				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD, ENV));
	}

	@Test
	void shouldNotRetryWhenFacingANonRetryableException() throws Exception {
		Call call = mock(Call.class);
		when(call.execute()).thenThrow(IOException.class);
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(IOException.class, () -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD, ENV));
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
				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD, ENV));
	}
}
