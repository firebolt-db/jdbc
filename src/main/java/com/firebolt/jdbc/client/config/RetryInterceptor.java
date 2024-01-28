package com.firebolt.jdbc.client.config;

import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

@RequiredArgsConstructor
@CustomLog
public class RetryInterceptor implements Interceptor {

	private static final Set<Integer> RETRYABLE_RESPONSE_CODES = new HashSet<>(
			Arrays.asList(HTTP_CLIENT_TIMEOUT, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE, HTTP_GATEWAY_TIMEOUT));

	private final int maxRetries;

	@NonNull
	@Override
	public Response intercept(@NonNull Chain chain) throws IOException {
		Request request = chain.request();
		Response response = chain.proceed(request);
		int tryCount = 0;
		while (!response.isSuccessful() && RETRYABLE_RESPONSE_CODES.contains(response.code())
				&& tryCount++ < maxRetries) {
			String failureInfo;
			String tag = request.tag(String.class);
			if (tag != null && !tag.isEmpty()) {
				failureInfo = String.format(
						"Failure #%d for query with id %s - Response code: %d. Retrying to send the request.", tryCount,
						tag, response.code());
			} else {
				failureInfo = String.format("Failure #%d - Response code: %d. Retrying to send the request.", tryCount,
						response.code());
			}
			log.warn(failureInfo);

			// retry the request
			response.close();
			response = chain.proceed(request);
		}

		return response;
	}
}
