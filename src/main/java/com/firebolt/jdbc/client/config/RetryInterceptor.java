package com.firebolt.jdbc.client.config;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.net.HttpURLConnection.*;

@RequiredArgsConstructor
@CustomLog
public class RetryInterceptor implements Interceptor {

    private static final Set<Integer> RETRYABLE_RESPONSE_CODES = new HashSet<>(Arrays.asList(HTTP_CLIENT_TIMEOUT,
            HTTP_BAD_GATEWAY,
            HTTP_UNAVAILABLE,
            HTTP_GATEWAY_TIMEOUT));

    private final int maxRetries;

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();
        Response response = chain.proceed(request);
        int tryCount = 0;
        while (!response.isSuccessful() && RETRYABLE_RESPONSE_CODES.contains(response.code()) && tryCount++ < maxRetries) {
            log.warn("Failure #{} - Response code: {}. Retrying to send the request.",
                    tryCount, response.code());
            // retry the request
            response = chain.call().clone().execute();
        }

        return response;
    }
}
