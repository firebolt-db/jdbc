package com.firebolt.jdbc.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.compress.LZ4OutputStream;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltClientTest {

	@Test
	void shouldThrowExceptionWhenResponseCodeIs401() {
		try (Response response = mockResponse(HTTP_UNAUTHORIZED)) {
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
			FireboltException exception = assertThrows(FireboltException.class,
					() -> client.validateResponse("host", response, false));
			assertEquals(ExceptionType.UNAUTHORIZED, exception.getType());
		}
	}

	@Test
	void shouldThrowExceptionWheResponseCodeIsOtherThan2XX() {
		try (Response response = mockResponse(HTTP_BAD_GATEWAY)) {
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
			FireboltException exception = assertThrows(FireboltException.class,
					() -> client.validateResponse("host", response, false));
			assertEquals(ExceptionType.ERROR, exception.getType());
		}
	}

	@Test
	void shouldNotThrowExceptionWhenResponseIs2XX() {
		try (Response response = mockResponse(HTTP_OK)) {
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
			assertAll(() -> client.validateResponse("host", response, false));
		}
	}

	@Test
	void shouldThrowExceptionWhenEngineIsNotRunning() {
		try (Response response = mockResponse(HTTP_UNAVAILABLE)) {
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			assertEquals("Could not query Firebolt at my_host. The engine is not running.", assertThrows(FireboltException.class, () -> client.validateResponse("my_host", response, true)).getMessage());
		}
	}

	@Test
	void shouldFailWhenCannotReadResponseBody() throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(500);
			ResponseBody responseBody = mock(ResponseBody.class);
			when(response.body()).thenReturn(responseBody);
			when(responseBody.bytes()).thenThrow(new IOException("ups"));
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			FireboltException e = assertThrows(FireboltException.class, () -> client.validateResponse("the_host", response, true));
			assertEquals(ExceptionType.ERROR, e.getType());
		}
	}

	@Test
	void cannotExtractCompressedErrorMessage() throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(HTTP_BAD_REQUEST);
			ResponseBody responseBody = mock(ResponseBody.class);
			when(response.body()).thenReturn(responseBody);
			when(responseBody.bytes()).thenReturn("ups".getBytes()); // compressed error message that uses wrong format
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			FireboltException e = assertThrows(FireboltException.class, () -> client.validateResponse("the_host", response, true));
			assertEquals(ExceptionType.INVALID_REQUEST, e.getType());
			assertTrue(e.getMessage().contains("ups")); // compressed error message is used as-is
		}
	}

	@Test
	void canExtractErrorMessage() throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(HTTP_NOT_FOUND);
			ResponseBody responseBody = mock(ResponseBody.class);
			when(response.body()).thenReturn(responseBody);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			OutputStream compressedStream = new LZ4OutputStream(baos, 100);
			compressedStream.write("Error happened".getBytes());
			compressedStream.flush();
			compressedStream.close();
			when(responseBody.bytes()).thenReturn(baos.toByteArray()); // compressed error message

			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			FireboltException e = assertThrows(FireboltException.class, () -> client.validateResponse("the_host", response, true));
			assertEquals(ExceptionType.RESOURCE_NOT_FOUND, e.getType());
			assertTrue(e.getMessage().contains("Error happened")); // compressed error message is used as-is
		}
	}

	@Test
	void emptyResponseFromServer() throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(200);
			when(response.body()).thenReturn(null);
			OkHttpClient okHttpClient = mock(OkHttpClient.class);
			Call call = mock();
			when(call.execute()).thenReturn(response);
			when(okHttpClient.newCall(any())).thenReturn(call);
			FireboltClient client = new FireboltClient(okHttpClient, mock(), null, null) {};
			assertEquals("Cannot get resource: the response from the server is empty", assertThrows(FireboltException.class, () -> client.getResource("http://foo", "foo", "token", String.class)).getMessage());
		}
	}

	private Response mockResponse(int code) {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		when(response.body()).thenReturn(responseBody);
		when(response.code()).thenReturn(code);
		return response;
	}
}
