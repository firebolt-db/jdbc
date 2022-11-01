package com.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.okhttp.FailsafeCall;
import okhttp3.*;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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

    @Test
    void shouldExecuteWithShallowCloneOfTheClientWhenConnectionPropertiesAreDifferentThanClientConfig() throws FireboltException, IOException {
        try (MockedStatic<FailsafeCall> mocked = mockStatic(FailsafeCall.class)) {
            OkHttpClient okHttpClient = mock(OkHttpClient.class);
            OkHttpClient shallowClient = mock(OkHttpClient.class);
            OkHttpClient.Builder shallowClientBuilder = mock(OkHttpClient.Builder.class);
            FireboltConnection fireboltConnection = mock(FireboltConnection.class);
            Response response = mock(Response.class);
            Request request = mock(Request.class);
            ResponseBody responseBody = mock(ResponseBody.class);
            Call call = mock(Call.class);
            FailsafeCall.FailsafeCallBuilder failsafeCallBuilder = mock(FailsafeCall.FailsafeCallBuilder.class);
            FailsafeCall failsafeCall = mock(FailsafeCall.class);

            when(fireboltConnection.getConnectionTimeout()).thenReturn(20);
            when(fireboltConnection.getNetworkTimeout()).thenReturn(10);
            when(okHttpClient.newBuilder()).thenReturn(shallowClientBuilder);
            when(shallowClientBuilder.readTimeout(any(Long.class), any(TimeUnit.class))).thenReturn(shallowClientBuilder);
            when(shallowClientBuilder.connectTimeout(any(Long.class), any(TimeUnit.class))).thenReturn(shallowClientBuilder);
            when(shallowClientBuilder.build()).thenReturn(shallowClient);
            when(response.body()).thenReturn(responseBody);
            when(response.code()).thenReturn(HTTP_ACCEPTED);
            mocked.when(() -> FailsafeCall.with(any(RetryPolicy.class))).thenReturn(failsafeCallBuilder);
            when(shallowClient.newCall(any())).thenReturn(call);
            when(failsafeCallBuilder.compose(call)).thenReturn(failsafeCall);
            when(failsafeCall.execute()).thenReturn(response);

            FireboltClient client = new StatementClientImpl(okHttpClient, fireboltConnection, mock(ObjectMapper.class), "", "", 3);
            client.execute(request, "host");

            verify(shallowClientBuilder).readTimeout(10, TimeUnit.MILLISECONDS);
            verify(shallowClientBuilder).connectTimeout(20, TimeUnit.MILLISECONDS);
            verify(shallowClientBuilder).build();
            verify(shallowClient).newCall(any());
            verify(failsafeCall).execute();
        }


    }
}
