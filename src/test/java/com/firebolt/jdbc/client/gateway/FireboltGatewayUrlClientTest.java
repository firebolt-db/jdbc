package com.firebolt.jdbc.client.gateway;

import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_ENTITY_TOO_LARGE;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_GONE;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_LENGTH_REQUIRED;
import static java.net.HttpURLConnection.HTTP_NOT_ACCEPTABLE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PAYMENT_REQUIRED;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static java.net.HttpURLConnection.HTTP_PROXY_AUTH;
import static java.net.HttpURLConnection.HTTP_REQ_TOO_LONG;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.net.HttpURLConnection.HTTP_UNSUPPORTED_TYPE;
import static java.net.HttpURLConnection.HTTP_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAccountRetrieverTest {
    private static final String GENERIC_ERROR_MESSAGE = "Server failed to execute query with the following error:";

	@Mock
	private OkHttpClient httpClient;

	@Mock
	private FireboltConnection fireboltConnection;

    private FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient;
    private FireboltAccountRetriever<FireboltAccount> fireboltAccountIdResolver;


    @BeforeEach
    void setUp() {
        fireboltGatewayUrlClient = new FireboltAccountRetriever<>(httpClient, fireboltConnection, null, null, "test-firebolt.io", "engineUrl", GatewayUrlResponse.class);
        fireboltAccountIdResolver = new FireboltAccountRetriever<>(httpClient, fireboltConnection, null, null, "test-firebolt.io", "resolve", FireboltAccount.class);
    }

	@Test
	void shouldGetGatewayUrlWhenResponseIsOk() throws SQLException, IOException {
        String engineUrl = "http://engine";
        injectMockedResponse(httpClient, HTTP_OK, format("{\"engineUrl\":  \"%s\"}", engineUrl));
        assertEquals(engineUrl, fireboltGatewayUrlClient.retrieve("access_token", "account").getEngineUrl());
	}

    @Test
    void shouldGetAccountId() throws SQLException, IOException {
        FireboltAccount account = new FireboltAccount("12345", "central", 2);
        injectMockedResponse(httpClient, HTTP_OK, "{\"id\": \"12345\", \"region\":\"central\", \"infraVersion\":2}");
        assertEquals(account, fireboltAccountIdResolver.retrieve("access_token", "account"));
    }

    @Test
    void shouldRuntimeExceptionUponRuntimeException() {
        when(httpClient.newCall(any())).thenThrow(new IllegalArgumentException("ex"));
        assertEquals("ex", assertThrows(IllegalArgumentException.class, () -> fireboltGatewayUrlClient.retrieve("token", "acc")).getMessage());
    }

    @Test
    void shouldThrowFireboltExceptionUponIOException() throws IOException {
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenThrow(new IOException("io error"));
        assertEquals("Failed to get engineUrl url for account acc: io error", assertThrows(FireboltException.class, () -> fireboltGatewayUrlClient.retrieve("token", "acc")).getMessage());
    }

    @ParameterizedTest(name = "{0}:{1}")
    @CsvSource({
            "resolve, com.firebolt.jdbc.client.account.FireboltAccount, {}, JSONObject[\"id\"] not found.",
            "engineUrl, com.firebolt.jdbc.client.gateway.GatewayUrlResponse, {}, JSONObject[\"engineUrl\"] not found."
    })
    <T> void shouldThrowFireboltExceptionUponWrongJsonFormat(String path, Class<T> clazz, String json, String expectedErrorMessage) throws IOException {
        FireboltAccountRetriever<T> fireboltAccountIdResolver = mockAccountRetriever(path, clazz, json);
        assertEquals(format("Failed to get %s url for account acc: %s", path, expectedErrorMessage), assertThrows(FireboltException.class, () -> fireboltAccountIdResolver.retrieve("token", "acc")).getMessage());
    }

    private <T> FireboltAccountRetriever<T> mockAccountRetriever(String path, Class<T> clazz, String json) throws IOException {
        try (Response response = mock(Response.class)) {
            when(response.code()).thenReturn(200);
            ResponseBody responseBody = mock(ResponseBody.class);
            when(responseBody.string()).thenReturn(json);
            when(response.body()).thenReturn(responseBody);
            OkHttpClient okHttpClient = mock(OkHttpClient.class);
            Call call = mock();
            when(call.execute()).thenReturn(response);
            when(okHttpClient.newCall(any())).thenReturn(call);
            return new FireboltAccountRetriever<>(okHttpClient, mock(), null, null, "test-firebolt.io", path, clazz);
        }
    }

    @ParameterizedTest
    @CsvSource({
            HTTP_BAD_REQUEST + "," + GENERIC_ERROR_MESSAGE,
            HTTP_PAYMENT_REQUIRED + "," + GENERIC_ERROR_MESSAGE,
            HTTP_FORBIDDEN + "," + GENERIC_ERROR_MESSAGE,
            HTTP_BAD_METHOD + "," + GENERIC_ERROR_MESSAGE,
            HTTP_NOT_ACCEPTABLE + "," + GENERIC_ERROR_MESSAGE,
            HTTP_PROXY_AUTH + "," + GENERIC_ERROR_MESSAGE,
            HTTP_CLIENT_TIMEOUT + "," + GENERIC_ERROR_MESSAGE,
            HTTP_CONFLICT + "," + GENERIC_ERROR_MESSAGE,
            HTTP_GONE + "," + GENERIC_ERROR_MESSAGE,
            HTTP_LENGTH_REQUIRED + "," + GENERIC_ERROR_MESSAGE,
            HTTP_PRECON_FAILED + "," + GENERIC_ERROR_MESSAGE,
            HTTP_ENTITY_TOO_LARGE + "," + GENERIC_ERROR_MESSAGE,
            HTTP_REQ_TOO_LONG + "," + GENERIC_ERROR_MESSAGE,
            HTTP_UNSUPPORTED_TYPE + "," + GENERIC_ERROR_MESSAGE,
            HTTP_INTERNAL_ERROR + "," + GENERIC_ERROR_MESSAGE,
            HTTP_NOT_IMPLEMENTED + "," + GENERIC_ERROR_MESSAGE,
            HTTP_BAD_GATEWAY + "," + GENERIC_ERROR_MESSAGE,
            HTTP_GATEWAY_TIMEOUT + "," + GENERIC_ERROR_MESSAGE,
            HTTP_VERSION + "," + GENERIC_ERROR_MESSAGE,

            HTTP_NOT_FOUND + "," + "Account '%s' does not exist",
            HTTP_UNAVAILABLE + "," + "Could not query Firebolt at https://test-firebolt.io/web/v3/account/%s/%s. The engine is not running.",
            HTTP_UNAUTHORIZED + "," + "Could not query Firebolt at https://test-firebolt.io/web/v3/account/%s/%s. The operation is not authorized"
    })
    void testFailedAccountDataRetrieving(int statusCode, String errorMessageTemplate) throws IOException {
        injectMockedResponse(httpClient, statusCode, null);
        assertErrorMessage(fireboltAccountIdResolver, "one", format(errorMessageTemplate, "one", "resolve"));
        assertErrorMessage(fireboltGatewayUrlClient, "two", format(errorMessageTemplate, "two", "engineUrl"));
    }

    private <T> void assertErrorMessage(FireboltAccountRetriever<T> accountRetriever, String accountName, String expectedErrorMessagePrefix) {
       MatcherAssert.assertThat(Assert.assertThrows(FireboltException.class, () -> accountRetriever.retrieve("access_token", accountName)).getMessage(), Matchers.startsWith(expectedErrorMessagePrefix));
    }

    private void injectMockedResponse(OkHttpClient httpClient, int code, String gatewayResponse) throws IOException {
        Response response = mock(Response.class);
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(response);
        when(response.code()).thenReturn(code);
        if (code == HTTP_OK) {
            ResponseBody body = mock(ResponseBody.class);
            when(response.body()).thenReturn(body);
            when(body.string()).thenReturn(gatewayResponse);
        }
    }
}