package com.firebolt.jdbc.client;

import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.compress.LZ4OutputStream;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltClientTest {

	private static final String URI = "https://api.firebolt.io/query";
	private static final String HOST = "api.firebolt.io";
	private static final String LABEL = "test-label";
	private static final String ACCESS_TOKEN = "test-token";
	private static final String SQL = "SELECT * FROM table";

	@Mock
	private OkHttpClient httpClient;

	@Mock
	private FireboltConnection connection;

	@Captor
	private ArgumentCaptor<Request> requestCaptor;

	private FireboltClient fireboltClient;

	@BeforeEach
	void setUp() {
		fireboltClient = new FireboltClient(httpClient, connection, null, null) {};
		lenient().when(connection.getConnectionTimeout()).thenReturn(1000);
		lenient().when(connection.getNetworkTimeout()).thenReturn(1000);
		lenient().when(httpClient.connectTimeoutMillis()).thenReturn(1000);
		lenient().when(httpClient.readTimeoutMillis()).thenReturn(1000);
	}

	@Test
	void shouldPostMultipartFormDataForParquetFilesWithSqlOnly() throws IOException, SQLException {
		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN);

		assertNotNull(result);
		verify(httpClient).newCall(requestCaptor.capture());
		Request request = requestCaptor.getValue();
		assertEquals(URI, request.url().toString());
		assertEquals(LABEL, request.tag());

		RequestBody body = request.body();
		assertNotNull(body);
        assertInstanceOf(MultipartBody.class, body);
		MultipartBody multipartBody = (MultipartBody) body;
		assertEquals(MultipartBody.FORM, multipartBody.type());

		assertEquals(1, multipartBody.parts().size());
		okhttp3.Headers sqlPartHeaders = multipartBody.parts().get(0).headers();
        assertNotNull(sqlPartHeaders);
        String contentDisposition = sqlPartHeaders.get("Content-Disposition");
        assertNotNull(contentDisposition);
        assertTrue(contentDisposition.contains("name=\"sql\""));
        assertFalse(contentDisposition.contains("filename"));
        
        MultipartBody.Part sqlPart = multipartBody.parts().get(0);
        MediaType sqlContentType = sqlPart.body().contentType();
        assertNotNull(sqlContentType);
        assertTrue(sqlContentType.toString().contains("text/plain"));
        assertTrue(sqlContentType.toString().contains("charset=utf-8"));
	}

	@Test
	void shouldPostMultipartFormDataForParquetFilesWithSqlAndFiles() throws IOException, SQLException {
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", "content1".getBytes(StandardCharsets.UTF_8));
		files.put("file2", "content2".getBytes(StandardCharsets.UTF_8));

		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, files, ACCESS_TOKEN);

		assertNotNull(result);
		verify(httpClient).newCall(requestCaptor.capture());
		Request request = requestCaptor.getValue();

		RequestBody body = request.body();
		assertNotNull(body);
        assertInstanceOf(MultipartBody.class, body);
		MultipartBody multipartBody = (MultipartBody) body;

		assertEquals(3, multipartBody.parts().size());

		boolean sqlPartFound = false;
		boolean file1Found = false;
		boolean file2Found = false;

		for (MultipartBody.Part part : multipartBody.parts()) {
            assertNotNull(part.headers());
            String contentDisposition = part.headers().get("Content-Disposition");
            assertNotNull(contentDisposition);
			if (contentDisposition.contains("name=\"sql\"")) {
				sqlPartFound = true;
                assertFalse(contentDisposition.contains("filename"));
                MediaType sqlContentType = part.body().contentType();
                assertNotNull(sqlContentType);
                assertTrue(sqlContentType.toString().contains("text/plain"));
                assertTrue(sqlContentType.toString().contains("charset=utf-8"));
			} else if (contentDisposition.contains("name=\"file1\"")) {
				file1Found = true;
				assertTrue(contentDisposition.contains("filename"));
				MediaType fileContentType = part.body().contentType();
				assertNotNull(fileContentType);
				assertTrue(fileContentType.toString().contains("application/octet-stream"));
			} else if (contentDisposition.contains("name=\"file2\"")) {
				file2Found = true;
				assertTrue(contentDisposition.contains("filename"));
				MediaType fileContentType = part.body().contentType();
				assertNotNull(fileContentType);
				assertTrue(fileContentType.toString().contains("application/octet-stream"));
			}
		}

		assertTrue(sqlPartFound, "SQL part should be present");
		assertTrue(file1Found, "file1 part should be present");
		assertTrue(file2Found, "file2 part should be present");
	}

	@Test
	void shouldPostMultipartFormDataForParquetFilesWithEmptyFilesMap() throws IOException, SQLException {
		Map<String, byte[]> files = new HashMap<>();

		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, files, ACCESS_TOKEN);

		assertNotNull(result);
		verify(httpClient).newCall(requestCaptor.capture());
		Request request = requestCaptor.getValue();

		RequestBody body = request.body();
		assertNotNull(body);
        assertInstanceOf(MultipartBody.class, body);
		MultipartBody multipartBody = (MultipartBody) body;
		assertEquals(1, multipartBody.parts().size());
	}

	@Test
	void shouldIncludeAuthorizationHeader() throws IOException, SQLException {
		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN);

		verify(httpClient).newCall(requestCaptor.capture());
		Request request = requestCaptor.getValue();
		assertEquals("Bearer " + ACCESS_TOKEN, request.header("Authorization"));
	}

	@Test
	void shouldWorkWithoutAccessToken() throws IOException, SQLException {
		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, null, null);

		assertNotNull(result);
		verify(httpClient).newCall(requestCaptor.capture());
		Request request = requestCaptor.getValue();
        assertNull(request.header("Authorization"));
	}

	@Test
	void shouldThrowIOException() throws IOException {
		Call call = mock(Call.class);
		when(call.execute()).thenThrow(new IOException("Network error"));
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(IOException.class, () ->
				fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN));
	}

	@Test
	void shouldThrowSQLException() throws IOException {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(500);
		ResponseBody responseBody = mock(ResponseBody.class);
		when(response.body()).thenReturn(responseBody);
		when(responseBody.bytes()).thenReturn("{\"errors\":[{\"message\":\"Server error\"}]}".getBytes());

		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		assertThrows(SQLException.class, () ->
				fireboltClient.postMultipartFormDataForParquetFiles(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN));
	}

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
			lenient().when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
			FireboltException exception = assertThrows(FireboltException.class,
					() -> client.validateResponse("host", response, false));
			assertEquals(ExceptionType.ERROR, exception.getType());
		}
	}

	@Test
	void shouldNotThrowExceptionWhenResponseIs2XX() {
		try (Response response = mockResponse(HTTP_OK)) {
			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			lenient().when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
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

	// FIR-33934: This test does not validate the fields of ServerError except error message including Location because this information is not exposed to FireboltException
	@ParameterizedTest
	@CsvSource(value = {
			"Error happened; Error happened",
			"Error happened on server: Line 16, Column 64: Something bad happened; Something bad happened",
			"{}; null",
			"{\"errors:\": [null]}; null",
			"{errors: [{\"name\": \"Something wrong happened\"}]}; Something wrong happened",
			"{errors: [{\"description\": \"Error happened on server: Line 16, Column 64: Something bad happened\"}]}; Something bad happened",
			"{errors: [{\"description\": \"Error happened on server: Line 16, Column 64: Something bad happened\", \"location\": {\"failingLine\": 20, \"startOffset\": 30, \"endOffset\": 40}}]}; Something bad happened"
	}, delimiter = ';')
	void canExtractErrorMessage(String rawMessage, String expectedMessage) throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(HTTP_NOT_FOUND);
			ResponseBody responseBody = mock(ResponseBody.class);
			when(response.body()).thenReturn(responseBody);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			OutputStream compressedStream = new LZ4OutputStream(baos, 100);
			compressedStream.write(rawMessage.getBytes());
			compressedStream.flush();
			compressedStream.close();
			when(responseBody.bytes()).thenReturn(baos.toByteArray()); // compressed error message

			FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
			FireboltException e = assertThrows(FireboltException.class, () -> client.validateResponse("the_host", response, true));
			assertEquals(ExceptionType.RESOURCE_NOT_FOUND, e.getType());
			assertTrue(e.getMessage().contains(expectedMessage)); // compressed error message is used as-is
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

	private static Stream<Arguments> goodJson() {
		return Stream.of(
				Arguments.of(GatewayUrlResponse.class, "{\"engineUrl\": \"my.engine\"}", new GatewayUrlResponse("my.engine")),
				Arguments.of(FireboltAccount.class, "{\"id\": \"123\", \"region\": \"earth\"}", new FireboltAccount("123", "earth", 1)),
				Arguments.of(GatewayUrlResponse.class, null, null),
				Arguments.of(FireboltAccount.class, null, null)
		);
	}

	@ParameterizedTest(name = "{0}:{1}")
	@MethodSource("goodJson")
	<T> void goodJsonResponse(Class<T> clazz, String json, T expected) throws SQLException, IOException {
		assertEquals(expected, mockClient(json).getResource("http://foo", "foo", "token", clazz));
	}

	private static Stream<Arguments> badJson() {
		return Stream.of(
				Arguments.of(GatewayUrlResponse.class, "", "A JSONObject text must begin with '{' at 0 [character 1 line 1]"),
				Arguments.of(FireboltAccount.class, "", "A JSONObject text must begin with '{' at 0 [character 1 line 1]"),
				Arguments.of(GatewayUrlResponse.class, "{}", "JSONObject[\"engineUrl\"] not found."),
				Arguments.of(FireboltAccount.class, "{}", "JSONObject[\"id\"] not found.")
		);
	}

	@ParameterizedTest(name = "{0}:{1}")
	@MethodSource("badJson")
	<T> void wrongJsonResponse(Class<T> clazz, String json, String expectedErrorMessage) throws IOException {
		FireboltClient client = mockClient(json);
		IOException e = assertThrows(IOException.class,() -> client.getResource("http://foo", "foo", "token", clazz));
		assertEquals(expectedErrorMessage, e.getMessage());
	}

	private FireboltClient mockClient(String json) throws IOException {
		try (Response response = mock(Response.class)) {
			when(response.code()).thenReturn(200);
			ResponseBody responseBody = mock(ResponseBody.class);
			when(responseBody.string()).thenReturn(json);
			when(response.body()).thenReturn(responseBody);
			OkHttpClient okHttpClient = mock(OkHttpClient.class);
			Call call = mock();
			when(call.execute()).thenReturn(response);
			when(okHttpClient.newCall(any())).thenReturn(call);
			return new FireboltClient(okHttpClient, mock(), null, null) {};
		}
	}

	private Response mockResponse(int code) {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		lenient().when(response.body()).thenReturn(responseBody);
		lenient().when(response.code()).thenReturn(code);
		return response;
	}
}
