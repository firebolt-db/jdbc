package com.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.CloseableUtil;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.*;

@Getter
@Slf4j
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
            assert response.body() != null : "Cannot get resource: the response from the server is empty";
            String responseStr = response.body().string();
            return objectMapper.readValue(responseStr, valueType);
        }
    }

    private Request createGetRequest(String uri, String accessToken) {
//		httpGet.setConfig(
//				createRequestConfig(this.connection.getConnectionTimeout(), this.connection.getNetworkTimeout()));

        Request.Builder requestBuilder = new Request.Builder().url(uri);
        this.createHeaders(accessToken).forEach(header -> requestBuilder.addHeader(header.getLeft(), header.getRight()));
        return requestBuilder.build();
    }

    protected Response execute(@NonNull Request request, String host)
            throws IOException, FireboltException {
        return execute(request, host, false);
    }

    protected Response execute(@NonNull Request request, String host,
                               boolean isCompress) throws IOException, FireboltException {
        Response response = null;
        try {
            response = this.getHttpClient().newCall(request).execute();
            validateResponse(host, response, isCompress);
        } catch (Exception e) {
            CloseableUtil.close(response);
            throw e;
        }
        return response;
    }

    protected Request createPostRequest(String uri, String body) {
        return createPostRequest(uri,  body, null, null);
    }

    protected Request createPostRequest(String uri, String accessToken, String body, String id) {
        Request.Builder requestBuilder = new Request.Builder().url(uri);
        this.createHeaders(accessToken).forEach(header -> requestBuilder.addHeader(header.getLeft(), header.getRight()));
        if (body != null) {
            requestBuilder.post(RequestBody.create(body, MediaType.parse("application/json")));
        }
        if (id != null) {
            requestBuilder.tag(id);
        }
        return requestBuilder.build();
    }

    protected void validateResponse(String host, Response response, Boolean isCompress)
            throws FireboltException {
        int statusCode = response.code();
        if (!isCallSuccessful(statusCode)) {
            if (statusCode == HTTP_UNAVAILABLE) {
                throw new FireboltException(
                        String.format("Could not query Firebolt at %s. The engine is not running. Status code: %d",
                                host, HTTP_FORBIDDEN),
                        statusCode);
            } else if (statusCode == HTTP_UNAUTHORIZED) {
                this.getConnection().removeExpiredTokens();
                throw new FireboltException(String.format(
                        "Could not query Firebolt at %s. The operation is not authorized or the token is expired and has been cleared from the cache",
						host), statusCode);
            }
            String errorResponseMessage;
            try {
                String errorFromResponse = extractErrorMessage(response, isCompress);
                errorResponseMessage = String.format(
                        "Server failed to execute query with the following error:%n%s%ninternal error:%n%s",
                        errorFromResponse, this.getInternalErrorWithHeadersText(response));
                throw new FireboltException(errorResponseMessage, statusCode);
            } catch (IOException e) {
                log.warn("Could not parse response containing the error message from Firebolt", e);
                errorResponseMessage = String.format("Server failed to execute query%ninternal error:%n%s",
                        this.getInternalErrorWithHeadersText(response));
                throw new FireboltException(errorResponseMessage, statusCode, e);
            }
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
