package com.firebolt.jdbc.client.gateway;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.firebolt.jdbc.exception.ExceptionType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;

@ExtendWith(MockitoExtension.class)
class FireboltGatewayUrlClientTest {

	@Spy
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Mock
	private OkHttpClient httpClient;

	@Mock
	private FireboltConnection fireboltConnection;

    @InjectMocks
    private FireboltGatewayUrlClient fireboltGatewayUrlClient;

	@Test
	void shouldGetGatewayUrlWhenResponseIsOk() throws IOException, FireboltException {
        GatewayUrlResponse response = GatewayUrlResponse.builder().engineUrl("http://engine").build();
        injectMockedResponse(httpClient, HTTP_OK, response);
        assertEquals("http://engine", fireboltGatewayUrlClient.getGatewayUrl("access_token", "account").getEngineUrl());
	}

    @Test
    void shouldThrowFireboltExceptionUponException() {
        when(httpClient.newCall(any())).thenThrow(new IllegalArgumentException("ex"));
        Exception ex = assertThrows(FireboltException.class, () ->fireboltGatewayUrlClient.getGatewayUrl("token", "acc"));
        assertEquals("Failed to get gateway url for account acc", ex.getMessage());
    }

    private void injectMockedResponse(OkHttpClient httpClient, int code, GatewayUrlResponse gatewayUrlResponse) throws IOException {
        Response response = mock(Response.class);
        Call call = mock(Call.class);
        when(httpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(response);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(response.code()).thenReturn(code);
        String gatewayResponse = new ObjectMapper()
                .writeValueAsString(gatewayUrlResponse);
        when(body.string()).thenReturn(gatewayResponse);
    }
}