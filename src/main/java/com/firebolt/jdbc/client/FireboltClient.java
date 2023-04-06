package com.firebolt.jdbc.client;

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.util.CloseableUtil;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;

import lombok.CustomLog;
import lombok.Getter;
import lombok.NonNull;
import okhttp3.*;

@Getter
@CustomLog
public abstract class FireboltClient {

	public static final String HEADER_AUTHORIZATION = "Authorization";
	public static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
	public static final String HEADER_USER_AGENT = "User-Agent";
	protected final ObjectMapper objectMapper;
	private final String headerUserAgentValue;
	private final OkHttpClient httpClient;
	private final FireboltConnection connection;

	protected FireboltClient(OkHttpClient httpClient, FireboltConnection connection, String customDrivers,
			String customClients, ObjectMapper objectMapper) {
		this.httpClient = httpClient;
		this.connection = connection;
		this.objectMapper = objectMapper;
		this.headerUserAgentValue = UsageTrackerUtil.getUserAgentString(customDrivers != null ? customDrivers : "",
				customClients != null ? customClients : "");
	}

	protected <T> T getResource(String uri, String host, String accessToken, Class<T> valueType)
			throws IOException, FireboltException {
		Request rq = createGetRequest(uri, accessToken);
		try (Response response = this.execute(rq, host)) {
			return objectMapper.readValue(getResponseAsString(response), valueType);
		}
	}

	private Request createGetRequest(String uri, String accessToken) {
		Request.Builder requestBuilder = new Request.Builder().url(uri);
		this.createHeaders(accessToken)
				.forEach(header -> requestBuilder.addHeader(header.getLeft(), header.getRight()));
		return requestBuilder.build();
	}

	protected Response execute(@NonNull Request request, String host) throws IOException, FireboltException {
		return execute(request, host, false);
	}

	protected Response execute(@NonNull Request request, String host, boolean isCompress)
			throws IOException, FireboltException {
		Response response = null;
		try {
			OkHttpClient client = getClientWithTimeouts(this.connection.getConnectionTimeout(),
					this.connection.getNetworkTimeout());
			Call call = client.newCall(request);
			response = call.execute();
			validateResponse(host, response, isCompress);
		} catch (Exception e) {
			CloseableUtil.close(response);
			throw e;
		}
		return response;
	}

	private OkHttpClient getClientWithTimeouts(int connectionTimeout, int networkTimeout) {
		if (connectionTimeout != this.httpClient.connectTimeoutMillis()
				|| networkTimeout != this.httpClient.readTimeoutMillis()) {
			// This creates a shallow copy using the same connection pool
			return this.httpClient.newBuilder().readTimeout(this.connection.getNetworkTimeout(), TimeUnit.MILLISECONDS)
					.connectTimeout(this.connection.getConnectionTimeout(), TimeUnit.MILLISECONDS).build();
		} else {
			return this.httpClient;
		}
	}

	protected Request createPostRequest(String uri, RequestBody requestBody) {
		return createPostRequest(uri, requestBody, null, null);
	}

	protected Request createPostRequest(String uri, RequestBody body, String accessToken, String id) {
		Request.Builder requestBuilder = new Request.Builder().url(uri);
		this.createHeaders(accessToken)
				.forEach(header -> requestBuilder.addHeader(header.getLeft(), header.getRight()));
		if (body != null) {
			requestBuilder.post(body);
		}
		if (id != null) {
			requestBuilder.tag(id);
		}
		return requestBuilder.build();
	}

	protected Request createPostRequest(String uri, String accessToken, String id) {
		return createPostRequest(uri, (RequestBody) null, accessToken, id);
	}

	protected Request createPostRequest(String uri, String json, String accessToken, String id) {
		RequestBody requestBody = null;
		if (json != null) {
			requestBody = RequestBody.create(json, MediaType.parse("application/json"));
		}
		return createPostRequest(uri, requestBody, accessToken, id);
	}

	protected void validateResponse(String host, Response response, Boolean isCompress) throws FireboltException {
		int statusCode = response.code();
		if (!isCallSuccessful(statusCode)) {
			if (statusCode == HTTP_UNAVAILABLE) {
				throw new FireboltException(
						String.format("Could not query Firebolt at %s. The engine is not running.", host), statusCode);
			}
			String errorResponseMessage;
			try {
				String errorMessageFromServer = extractErrorMessage(response, isCompress);
				errorResponseMessage = String.format(
						"Server failed to execute query with the following error:%n%s%ninternal error:%n%s",
						errorMessageFromServer, this.getInternalErrorWithHeadersText(response));
				if (statusCode == HTTP_UNAUTHORIZED) {
					this.getConnection().removeExpiredTokens();
					throw new FireboltException(String.format(
							"Could not query Firebolt at %s. The operation is not authorized or the token is expired and has been cleared from the cache.%n%s",
							host, errorResponseMessage), statusCode, errorMessageFromServer);
				}
				throw new FireboltException(errorResponseMessage, statusCode, errorMessageFromServer);
			} catch (IOException e) {
				log.warn("Could not parse response containing the error message from Firebolt", e);
				errorResponseMessage = String.format("Server failed to execute query%ninternal error:%n%s",
						this.getInternalErrorWithHeadersText(response));
				throw new FireboltException(errorResponseMessage, statusCode, e);
			}
		}
	}

	protected String getResponseAsString(Response response) throws FireboltException, IOException {
		if (response.body() == null) {
			throw new FireboltException("Cannot get resource: the response from the server is empty");
		} else {
			return response.body().string();
		}
	}

	private String extractErrorMessage(Response response, boolean isCompress) throws IOException {
		byte[] entityBytes;
		if (response.body() != null) {
			entityBytes = response.body().bytes();
		} else {
			entityBytes = null;
		}
		if (isCompress && entityBytes != null) {
			try {
				InputStream is = new LZ4InputStream(new ByteArrayInputStream(entityBytes));
				return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
						.collect(Collectors.joining("\n")) + "\n";
			} catch (Exception e) {
				log.warn("Could not decompress error from server");
			}
		}
		return entityBytes != null ? new String(entityBytes, StandardCharsets.UTF_8) : null;
	}

	private boolean isCallSuccessful(int statusCode) {
		return statusCode >= 200 && statusCode <= 299; // Call is considered successful when the status code is 2XX
	}

	private List<Pair<String, String>> createHeaders(String accessToken) {
		List<Pair<String, String>> headers = new ArrayList<>();
		headers.add(new ImmutablePair<>(HEADER_USER_AGENT, this.getHeaderUserAgentValue()));
		Optional.ofNullable(accessToken).ifPresent(token -> headers.add(
				new ImmutablePair<>(HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken)));
		return headers;
	}

	private String getInternalErrorWithHeadersText(Response response) {
		return response.toString() + "\n" + response.headers();
	}

}
