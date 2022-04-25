package io.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.ProjectVersionUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

@Getter
@Slf4j
public abstract class FireboltClient {

    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
    public static final String HEADER_USER_AGENT = "User-Agent";
    private final String headerUserAgentValue;

    protected FireboltClient() {
        Optional<String> version = ProjectVersionUtil.getProjectVersion();
        if (!version.isPresent()) {
            log.warn("Could not fetch project version from version.properties. The User-Agent header will not contain the version of the driver.");
        }
        this.headerUserAgentValue = String.format("fireboltJdbcDriver%s", version.map(s -> "/" + s).orElse(StringUtils.EMPTY));
    }

    protected <T> T getResource(String uri, String identifier, String accessToken, CloseableHttpClient httpClient, ObjectMapper objectMapper, Class<T> valueType) throws IOException {
        HttpGet httpGet = createGetRequest(uri, accessToken);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            int statusCode = Optional.ofNullable(response.getStatusLine()).map(StatusLine::getStatusCode).orElse(-1);
            String responseStr = EntityUtils.toString(response.getEntity());
            log.debug("GET {} - Http status code : {}, response : {}", uri, statusCode, responseStr);

            if (HttpURLConnection.HTTP_OK != statusCode) {
                if (statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    throw new IOException(String.format("Could not find resource with identifier %s, uri: %s, response: %s", identifier, uri, responseStr));
                }
                throw new IOException(String.format("Failed to connect to Firebolt. status code: %d, Response: %s", statusCode, responseStr));
            }
            return objectMapper.readValue(responseStr, valueType);
        }
    }

    protected String createUri(String account, String prefix, String suffix) {
        if (StringUtils.isNotEmpty(account))
            return prefix + "s/" + account + suffix;
        else
            return prefix + suffix;
    }

    private HttpGet createGetRequest(String uri, String accessToken) {
        HttpGet httpGet = new HttpGet(uri);
        httpGet.addHeader(HEADER_USER_AGENT, this.getHeaderUserAgentValue());
        httpGet.addHeader(HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken);
        return httpGet;
    }

}
