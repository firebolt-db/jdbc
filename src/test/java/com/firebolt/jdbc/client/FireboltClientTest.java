package com.firebolt.jdbc.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static java.net.HttpURLConnection.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltClientTest {

	@Test
	void shouldThrowExceptionWhenResponseCodeIs401() {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		when(response.body()).thenReturn(responseBody);
		when(response.code()).thenReturn(HTTP_UNAUTHORIZED);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		FireboltException exception = assertThrows(FireboltException.class,
				() -> client.validateResponse("host", response, false));
		assertEquals(ExceptionType.UNAUTHORIZED, exception.getType());
	}

	@Test
	void shouldThrowExceptionWheResponseCodeIsOtherThan2XX() {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		when(response.body()).thenReturn(responseBody);
		when(response.code()).thenReturn(HTTP_BAD_GATEWAY);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		FireboltException exception = assertThrows(FireboltException.class,
				() -> client.validateResponse("host", response, false));
		assertEquals(ExceptionType.ERROR, exception.getType());
	}

	@Test
	void shouldNotThrowExceptionWhenResponseIs2XX() {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		when(response.body()).thenReturn(responseBody);
		when(response.code()).thenReturn(HTTP_OK);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		assertAll(() -> client.validateResponse("host", response, false));
	}
}
