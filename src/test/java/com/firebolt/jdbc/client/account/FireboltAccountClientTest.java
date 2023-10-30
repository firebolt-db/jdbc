package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.HttpURLConnection;

import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static com.firebolt.jdbc.exception.ExceptionType.TOO_MANY_REQUESTS;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAccountClientTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Mock
    private OkHttpClient httpClient;
    @Mock
    private FireboltConnection fireboltConnection;
    private FireboltAccountClient client;

    @BeforeEach
    void setUp() {
        client = new FireboltAccountClient(httpClient, objectMapper, fireboltConnection, null, null);
    }

    @Test
    void getAccount() throws FireboltException, IOException {
        String accountId = "123";
        injectMockedResponse(httpClient, HTTP_OK, new FireboltAccountResponse(accountId));
        assertEquals(accountId, client.getAccount("http://host", "account", "token").getAccountId());
    }

    @Test
    void getEngine() throws IOException, FireboltException {
        String endpoint = "http://engine/12345";
        FireboltEngineResponse response = FireboltEngineResponse.builder().engine(FireboltEngineResponse.Engine.builder().currentStatus("running").endpoint(endpoint).build()).build();
        injectMockedResponse(httpClient, HTTP_OK, response);
        assertEquals(endpoint, client.getEngine("http://host", "account-id", "engine", "engine-id", "token").getEngine().getEndpoint());
    }

    @Test
    void getDefaultEngineByDatabaseName() throws FireboltException, IOException {
        String endpoint = "http://engine/12345";
        FireboltDefaultDatabaseEngineResponse response = FireboltDefaultDatabaseEngineResponse.builder().engineUrl(endpoint).build();
        injectMockedResponse(httpClient, HTTP_OK, response);
        assertEquals(endpoint, client.getDefaultEngineByDatabaseName("http://host", "account-id", "db", "token").getEngineUrl());
    }

    @Test
    void getEngineId() throws FireboltException, IOException {
        String engineId = "456";
        FireboltEngineIdResponse response = FireboltEngineIdResponse.builder().engine(FireboltEngineIdResponse.Engine.builder().engineId(engineId).build()).build();
        injectMockedResponse(httpClient, HTTP_OK, response);
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

    private void injectMockedResponse(OkHttpClient httpClient, int code, Object payload) throws IOException {
        Response response = mock(Response.class);
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(response);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(response.code()).thenReturn(code);
        String gatewayResponse = new ObjectMapper().writeValueAsString(payload);
        lenient().when(body.string()).thenReturn(gatewayResponse);
    }
}