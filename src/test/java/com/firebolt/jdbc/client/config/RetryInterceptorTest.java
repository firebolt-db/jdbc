package com.firebolt.jdbc.client.config;

import okhttp3.Call;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static java.net.HttpURLConnection.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryInterceptorTest {

    @ParameterizedTest
    @ValueSource(ints = {HTTP_CLIENT_TIMEOUT, HTTP_INTERNAL_ERROR, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE,
            HTTP_GATEWAY_TIMEOUT})
    void shouldRetryOnRetryableResponseCode(int arg) throws IOException {
        int retries = 3;
        RetryInterceptor retryInterceptor = new RetryInterceptor(retries);
        Interceptor.Chain chain = mock(Interceptor.Chain.class);
        Response response = mock(Response.class);
        Call call = mock(Call.class);
        Request request = mock(Request.class);

        when(chain.request()).thenReturn(request);
        when(chain.proceed(any(Request.class))).thenReturn(response);
        when(response.isSuccessful()).thenReturn(false);
        when(response.code()).thenReturn(arg);
        when(chain.call()).thenReturn(call);

        retryInterceptor.intercept(chain);
        verify(chain, times(1 + retries)).proceed(request);
    }

    @Test
    void shouldNotRetryOnNonRetryableResponseCode() throws IOException {
        int retries = 0;
        RetryInterceptor retryInterceptor = new RetryInterceptor(retries);
        Interceptor.Chain chain = mock(Interceptor.Chain.class);
        Response response = mock(Response.class);
        Call retryCall = mock(Call.class);

        when(chain.request()).thenReturn(mock(Request.class));
        when(chain.proceed(any(Request.class))).thenReturn(response);
        when(response.isSuccessful()).thenReturn(false);
        when(response.code()).thenReturn(HTTP_NOT_FOUND);

        retryInterceptor.intercept(chain);
        verify(retryCall, times(0)).execute();
    }
}