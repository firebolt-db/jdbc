package com.firebolt.jdbc.client.discovery;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.util.CloseableUtil;
import java.io.IOException;
import java.sql.SQLException;
import lombok.RequiredArgsConstructor;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.json.JSONObject;

@RequiredArgsConstructor
public class FireboltDiscoveryClient {

    private static final String WELL_KNOWN_PATH = ".well-known";
    private static final String FIREBOLT_DISCOVERY_PATH = "firebolt";

    private final OkHttpClient httpClient;

    public FireboltDiscoveryDocument discover(FireboltProperties properties) throws SQLException {
        HttpUrl discoveryUrl = new HttpUrl.Builder()
                .scheme(properties.isSsl() ? "https" : "http")
                .host(properties.getHost())
                .port(properties.getPort())
                .addPathSegment(WELL_KNOWN_PATH)
                .addPathSegment(FIREBOLT_DISCOVERY_PATH)
                .build();
        Request request = new Request.Builder().url(discoveryUrl).get().build();
        Response response = null;
        try {
            response = httpClient.newCall(request).execute();
            if (!response.isSuccessful()) {
                throw new FireboltException(String.format("Failed to discover Firebolt endpoint at %s: HTTP %s",
                        discoveryUrl, response.code()));
            }
            ResponseBody responseBody = response.body();
            if (responseBody == null) {
                throw new FireboltException(String.format("Failed to discover Firebolt endpoint at %s: empty response",
                        discoveryUrl));
            }
            return new FireboltDiscoveryDocument(new JSONObject(responseBody.string()));
        } catch (IOException | RuntimeException e) {
            throw new FireboltException(String.format("Failed to discover Firebolt endpoint at %s", discoveryUrl), e);
        } finally {
            CloseableUtil.close(response);
        }
    }
}
