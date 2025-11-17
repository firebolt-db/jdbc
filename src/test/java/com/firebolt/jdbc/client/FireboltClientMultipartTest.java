package com.firebolt.jdbc.client;

import com.firebolt.jdbc.connection.FireboltConnection;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_OK;
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
class FireboltClientMultipartTest {

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
		when(connection.getConnectionTimeout()).thenReturn(1000);
		when(connection.getNetworkTimeout()).thenReturn(1000);
		when(httpClient.connectTimeoutMillis()).thenReturn(1000);
		when(httpClient.readTimeoutMillis()).thenReturn(1000);
	}

	@Test
	void shouldPostMultipartFormDataWithSqlOnly() throws IOException, SQLException {
		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN);

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
        assertTrue(sqlPartHeaders.toString().contains("name=\"sql\""));
        assertFalse(sqlPartHeaders.toString().contains("filename"));
        
        MultipartBody.Part sqlPart = multipartBody.parts().get(0);
        MediaType sqlContentType = sqlPart.body().contentType();
        assertNotNull(sqlContentType);
        assertTrue(sqlContentType.toString().contains("text/plain"));
        assertTrue(sqlContentType.toString().contains("charset=utf-8"));
	}

	@Test
	void shouldPostMultipartFormDataWithSqlAndFiles() throws IOException, SQLException {
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", "content1".getBytes(StandardCharsets.UTF_8));
		files.put("file2", "content2".getBytes(StandardCharsets.UTF_8));

		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, files, ACCESS_TOKEN);

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
            String headers = part.headers().toString();
			if (headers.contains("name=\"sql\"")) {
				sqlPartFound = true;
                assertFalse(headers.contains("filename"));
                MediaType sqlContentType = part.body().contentType();
                assertNotNull(sqlContentType);
                assertTrue(sqlContentType.toString().contains("text/plain"));
                assertTrue(sqlContentType.toString().contains("charset=utf-8"));
			} else if (headers.contains("name=\"file1\"")) {
				file1Found = true;
				assertTrue(headers.contains("filename"));
				MediaType fileContentType = part.body().contentType();
				assertNotNull(fileContentType);
				assertTrue(fileContentType.toString().contains("application/octet-stream"));
			} else if (headers.contains("name=\"file2\"")) {
				file2Found = true;
				assertTrue(headers.contains("filename"));
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
	void shouldPostMultipartFormDataWithEmptyFilesMap() throws IOException, SQLException {
		Map<String, byte[]> files = new HashMap<>();

		Response response = mockResponse(HTTP_OK);
		Call call = mock(Call.class);
		when(call.execute()).thenReturn(response);
		when(httpClient.newCall(any())).thenReturn(call);

		Response result = fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, files, ACCESS_TOKEN);

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

		fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN);

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

		Response result = fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, null, null);

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
				fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN));
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
				fireboltClient.postMultipartFormData(URI, HOST, LABEL, SQL, null, ACCESS_TOKEN));
	}

	private Response mockResponse(int statusCode) {
		Response response = mock(Response.class);
		ResponseBody responseBody = mock(ResponseBody.class);
		lenient().when(response.body()).thenReturn(responseBody);
		lenient().when(response.code()).thenReturn(statusCode);
		return response;
	}
}

