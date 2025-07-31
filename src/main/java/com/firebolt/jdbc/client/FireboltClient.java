package com.firebolt.jdbc.client;

import com.firebolt.jdbc.connection.CacheListener;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.SQLState;
import com.firebolt.jdbc.exception.ServerError;
import com.firebolt.jdbc.exception.ServerError.Error.Location;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import com.firebolt.jdbc.util.CloseableUtil;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.CustomLog;
import lombok.Getter;
import lombok.NonNull;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONException;
import org.json.JSONObject;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Optional.ofNullable;

@Getter
@CustomLog
public abstract class FireboltClient implements CacheListener {
	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
	private static final String HEADER_USER_AGENT = "User-Agent";
	private static final String HEADER_PROTOCOL_VERSION = "Firebolt-Protocol-Version";
	private static final Pattern plainErrorPattern = Pattern.compile("Line (\\d+), Column (\\d+): (.*)$", Pattern.MULTILINE);
	private final OkHttpClient httpClient;
	private final String headerUserAgentValue;
	protected final FireboltConnection connection;

	protected FireboltClient(OkHttpClient httpClient, FireboltConnection connection, String customDrivers, String customClients) {
		this.httpClient = httpClient;
		this.connection = connection;
		this.headerUserAgentValue = UsageTrackerUtil.getUserAgentString(customDrivers != null ? customDrivers : "",
				customClients != null ? customClients : "", connection.getConnectionUserAgentHeader().orElse(null));
		connection.register(this);
	}

	protected <T> T getResource(String uri, String accessToken, Class<T> valueType)
			throws IOException, SQLException {
		return getResource(uri, uri, accessToken, valueType);
	}

	protected <T> T getResource(String uri, String host, String accessToken, Class<T> valueType) throws SQLException, IOException {
		Request rq = createGetRequest(uri, accessToken);
		try (Response response = execute(rq, host)) {
			return jsonToObject(getResponseAsString(response), valueType);
		}
	}

	@SuppressWarnings("java:S3011") // setAccessible() is required here :(
	protected <T> T jsonToObject(String json, Class<T> valueType) throws IOException {
        try {
			Constructor<T> constructor = valueType.getDeclaredConstructor(JSONObject.class);
			constructor.setAccessible(true);
            return json == null ? null : constructor.newInstance(new JSONObject(json));
        } catch (ReflectiveOperationException | RuntimeException e) {
			Throwable cause = Optional.ofNullable(e.getCause()).orElse(e);
			throw new IOException(cause.getMessage(), cause);
        }
    }

	private Request createGetRequest(String uri, String accessToken) {
		Request.Builder requestBuilder = new Request.Builder().url(uri);
		createHeaders(accessToken).forEach(header -> requestBuilder.addHeader(header.getKey(), header.getValue()));
		return requestBuilder.build();
	}

	protected Response execute(@NonNull Request request, String host) throws IOException, SQLException {
		return execute(request, host, false);
	}

	protected Response execute(@NonNull Request request, String host, boolean isCompress)
			throws IOException, SQLException {
		Response response = null;
		try {
			OkHttpClient client = getClientWithTimeouts(connection.getConnectionTimeout(), connection.getNetworkTimeout());
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
		if (connectionTimeout != httpClient.connectTimeoutMillis()
				|| networkTimeout != httpClient.readTimeoutMillis()) {
			// This creates a shallow copy using the same connection pool
			return httpClient.newBuilder().readTimeout(connection.getNetworkTimeout(), TimeUnit.MILLISECONDS)
					.connectTimeout(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS).build();
		} else {
			return httpClient;
		}
	}

	protected Request createPostRequest(String uri, String label, RequestBody body, String accessToken) {
		Request.Builder requestBuilder = new Request.Builder().url(uri).tag(label);
		createHeaders(accessToken).forEach(header -> requestBuilder.addHeader(header.getKey(), header.getValue()));
		if (body != null) {
			requestBuilder.post(body);
		}
		return requestBuilder.build();
	}

	protected Request createPostRequest(String uri, String label, String json, String accessToken) {
		RequestBody requestBody = null;
		if (json != null) {
			requestBody = RequestBody.create(json, MediaType.parse("application/json"));
		}
		return createPostRequest(uri, label, requestBody, accessToken);
	}

	protected void validateResponse(String host, Response response, Boolean isCompress) throws SQLException {
		int statusCode = response.code();
		if (!isCallSuccessful(statusCode)) {
			if (statusCode == HTTP_UNAVAILABLE) {
				throw new FireboltException(format("Could not query Firebolt at %s. The engine is not running.", host),
						statusCode, SQLState.CONNECTION_FAILURE);
			}
			String errorMessageFromServer = extractErrorMessage(response, isCompress);
			ServerError serverError = parseServerError(errorMessageFromServer);
			String processedErrorMessage = serverError.getErrorMessage();
			validateResponse(host, statusCode, processedErrorMessage);
			String errorResponseMessage = format(
					"Server failed to execute query with the following error:%n%s%ninternal error:%n%s",
					processedErrorMessage, getInternalErrorWithHeadersText(response));
			if (statusCode == HTTP_UNAUTHORIZED || statusCode == HTTP_FORBIDDEN) {
				getConnection().removeExpiredTokens();
				throw new FireboltException(format(
						"Could not query Firebolt at %s. The operation is not authorized or the token is expired and has been cleared from the cache.%n%s",
						host, errorResponseMessage), statusCode, processedErrorMessage,
						SQLState.INVALID_AUTHORIZATION_SPECIFICATION);
			}
			throw new FireboltException(errorResponseMessage, statusCode, processedErrorMessage);
		}
	}

	protected void validateResponse(String host, int statusCode, String errorMessageFromServer) throws SQLException {
		// empty implementation
	}

	protected String getResponseAsString(Response response) throws SQLException, IOException {
		if (response.body() == null) {
			throw new FireboltException("Cannot get resource: the response from the server is empty");
		}
		return response.body().string();
	}

	private String extractErrorMessage(Response response, boolean isCompress) throws SQLException {
		byte[] entityBytes;
		try {
			entityBytes = response.body() !=  null ? response.body().bytes() : null;
		} catch (IOException e) {
			log.warn("Could not parse response containing the error message from Firebolt", e);
			String errorResponseMessage = format("Server failed to execute query%ninternal error:%n%s",
					getInternalErrorWithHeadersText(response));
			throw new FireboltException(errorResponseMessage, response.code(), e);
		}

		if (entityBytes == null) {
			return null;
		}
		if (isCompress) {
			try {
				InputStream is = new LZ4InputStream(new ByteArrayInputStream(entityBytes));
				return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
						.collect(Collectors.joining("\n")) + "\n";
			} catch (Exception e) {
				log.warn("Could not decompress error from server");
			}
		}
		return new String(entityBytes, StandardCharsets.UTF_8);
	}

	private ServerError parseServerError(String responseText) {
		try {
			if (responseText == null) {
				return new ServerError(null, null);
			}
			ServerError serverError = new ServerError(new JSONObject(responseText));
			ServerError.Error[] errors = serverError.getErrors() == null ? null : Arrays.stream(serverError.getErrors()).map(this::updateError).toArray(ServerError.Error[]::new);
			return new ServerError(serverError.getQuery(), errors);
		} catch (JSONException e) {
			Location location = getLocationFromMessage(responseText);
			return new ServerError(null, new ServerError.Error[] {new ServerError.Error(null, responseText, null, null, null, null, null, location)});
		}
	}

	private ServerError.Error updateError(ServerError.Error error) {
		if (error == null || error.getDescription() == null) {
			return error;
		}
		Location location = getLocationFromMessage(error.getDescription());
		if (location == null) {
			return error;
		}
		location = Objects.requireNonNullElse(error.getLocation(), location);
		return new ServerError.Error(error.getCode(), error.getName(), error.getSeverity(), error.getSource(),
				error.getDescription(), error.getResolution(), error.getHelpLink(), location);
	}

	private Location getLocationFromMessage(String responseText) {
		Matcher m = plainErrorPattern.matcher(responseText);
		if (m.find()) {
			int line = Integer.parseInt(m.group(1));
			int column = Integer.parseInt(m.group(2));
			return new Location(line, column, column);
		}
		return null;
	}

	protected boolean isCallSuccessful(int statusCode) {
		return statusCode >= 200 && statusCode <= 299; // Call is considered successful when the status code is 2XX
	}

	private List<Entry<String, String>> createHeaders(String accessToken) {
		List<Entry<String, String>> headers = new ArrayList<>();
		headers.add(Map.entry(HEADER_USER_AGENT, headerUserAgentValue));
		ofNullable(connection.getProtocolVersion()).ifPresent(version -> headers.add(Map.entry(HEADER_PROTOCOL_VERSION, version)));
		ofNullable(accessToken).ifPresent(token -> headers.add(Map.entry(HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken)));
		return headers;
	}

	private String getInternalErrorWithHeadersText(Response response) {
		return response.toString() + "\n" + response.headers();
	}

	@Override
	public void cleanup() {
		// empty default implementation
	}
}
