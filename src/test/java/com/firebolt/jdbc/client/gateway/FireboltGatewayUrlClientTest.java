package com.firebolt.jdbc.client.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAccountRetrieverTest {

	@Spy
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Mock
	private OkHttpClient httpClient;

	@Mock
	private FireboltConnection fireboltConnection;

    private FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient;
    private FireboltAccountRetriever<FireboltAccount> fireboltAccountIdResolver;


    @BeforeEach
    void setUp() {
        fireboltGatewayUrlClient = new FireboltAccountRetriever<>(httpClient, objectMapper, fireboltConnection, null, null, "http://test-firebolt.io", "engineUrl", GatewayUrlResponse.class);
        fireboltAccountIdResolver = new FireboltAccountRetriever<>(httpClient, objectMapper, fireboltConnection, null, null, "http://test-firebolt.io", "resolve", FireboltAccount.class);
    }

	@Test
	void shouldGetGatewayUrlWhenResponseIsOk() throws IOException, FireboltException {
        GatewayUrlResponse response = GatewayUrlResponse.builder().engineUrl("http://engine").build();
        injectMockedResponse(httpClient, HTTP_OK, response);
        assertEquals("http://engine", fireboltGatewayUrlClient.retrieve("access_token", "account").getEngineUrl());
	}

    @Test
    void shouldGetAccountId() throws IOException, FireboltException {
        FireboltAccount account = new FireboltAccount("12345", "central");
        injectMockedResponse(httpClient, HTTP_OK, account);
        assertEquals(account, fireboltAccountIdResolver.retrieve("access_token", "account"));
    }

    @Test
    void shouldRuntimeExceptionUponRuntimeException() throws FireboltException {
        when(httpClient.newCall(any())).thenThrow(new IllegalArgumentException("ex"));
        assertEquals("ex", assertThrows(IllegalArgumentException.class, () -> fireboltGatewayUrlClient.retrieve("token", "acc")).getMessage());
    }

    @Test
    void shouldThrowFireboltExceptionUponIOException() throws IOException {
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenThrow(new IOException("ex"));
        assertEquals("Failed to get engineUrl url for account acc", assertThrows(FireboltException.class, () -> fireboltGatewayUrlClient.retrieve("token", "acc")).getMessage());
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
        when(body.string()).thenReturn(gatewayResponse);
    }
}