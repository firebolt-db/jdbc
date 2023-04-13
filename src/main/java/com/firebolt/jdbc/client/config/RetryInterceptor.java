package com.firebolt.jdbc.client.config;

import static java.net.HttpURLConnection.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

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
			if (request.tag() instanceof String && StringUtils.isNotEmpty((String) request.tag())) {
				failureInfo = String.format(
						"Failure #%d for query with id %s - Response code: %d. Retrying to send the request.", tryCount,
						request.tag(), response.code());
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
