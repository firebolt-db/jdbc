package com.firebolt.jdbc.client.account;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;

import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAccountClientTest {
    @Mock
    private OkHttpClient httpClient;
    @Mock
    private FireboltConnection fireboltConnection;
    private FireboltAccountClient client;

    @BeforeEach
    void setUp() {
        client = new FireboltAccountClient(httpClient, fireboltConnection, null, null);
    }

    @Test
    void getAccount() throws SQLException, IOException {
        client.cleanup();
        String accountId = "123";
        injectMockedResponse(httpClient, HTTP_OK, format("{\"account_id\":\"%s\"}", accountId)); // FireboltAccountResponse
        assertEquals(accountId, client.getAccount("http://host", "account", "token").getAccountId());
        verify(httpClient, times(1)).newCall(any());
        assertEquals(accountId, client.getAccount("http://host", "account", "token").getAccountId());
        verify(httpClient, times(1)).newCall(any());
        client.cleanup();
        assertEquals(accountId, client.getAccount("http://host", "account", "token").getAccountId());
        verify(httpClient, times(2)).newCall(any());
    }

    @Test
    void getEngine() throws SQLException, IOException {
        String endpoint = "http://engine/12345";
        injectMockedResponse(httpClient, HTTP_OK, format("{\"engine\": {\"endpoint\": \"%s\", \"current_status\": \"%s\"}}", endpoint, "running")); // FireboltEngineResponse
        assertEquals("http://engine/12345", client.getEngine("http://host", "account-id", "engine", "engine-id", "token").getEngine().getEndpoint());
    }

    @Test
    void getDefaultEngineByDatabaseName() throws SQLException, IOException {
        String endpoint = "http://engine/12345";
        injectMockedResponse(httpClient, HTTP_OK, format("{\"engine_url\": \"%s\"}", endpoint)); // FireboltDefaultDatabaseEngineResponse
        assertEquals(endpoint, client.getDefaultEngineByDatabaseName("http://host", "account-id", "db", "token").getEngineUrl());
    }

    @Test
    void getEngineId() throws SQLException, IOException {
        String engineId = "456";
        injectMockedResponse(httpClient, HTTP_OK, format("{\"engine_id\": {\"engine_id\":\"%s\"}}", engineId)); // FireboltEngineIdResponse
        assertEquals(engineId, client.getEngineId("http://host", "account-id", "db", "token").getEngine().getEngineId());
    }

    @Test
    void notFoundError() throws IOException {
        injectMockedResponse(httpClient, HTTP_NOT_FOUND, null);
        assertNotFoundException(assertThrows(FireboltException.class, () -> client.getEngine("http://host", "account-id", "engine", "engine-id", "token")));
        assertNotFoundException(assertThrows(FireboltException.class, () -> client.getDefaultEngineByDatabaseName("http://host", "account-id", "db", "token")));
        assertNotFoundException(assertThrows(FireboltException.class, () -> client.getEngineId("http://host", "account-id", "db", "token")));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "404, RESOURCE_NOT_FOUND",
            "400, INVALID_REQUEST",
            "401, UNAUTHORIZED",
            "429, TOO_MANY_REQUESTS",
            "406, ERROR"
    })
    void httpFailures(int httpStatus, ExceptionType type) throws IOException {
        injectMockedResponse(httpClient, httpStatus, null);
        assertEquals(type, assertThrows(FireboltException.class, () -> client.getEngine("http://host", "account-id", "engine", "engine-id", "token")).getType());
        assertEquals(type, assertThrows(FireboltException.class, () -> client.getDefaultEngineByDatabaseName("http://host", "account-id", "db", "token")).getType());
        assertEquals(type, assertThrows(FireboltException.class, () -> client.getEngineId("http://host", "account-id", "db", "token")).getType());
    }

    private void assertNotFoundException(FireboltException e) {
        assertEquals(RESOURCE_NOT_FOUND, e.getType());
        assertTrue(e.getMessage().contains("could not be found"));
    }

    private void injectMockedResponse(OkHttpClient httpClient, int code, String json) throws IOException {
        Response response = mock(Response.class);
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(response);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(response.code()).thenReturn(code);
        lenient().when(body.string()).thenReturn(json);
    }
}