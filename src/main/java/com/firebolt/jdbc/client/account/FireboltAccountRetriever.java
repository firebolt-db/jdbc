package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.OkHttpClient;

import java.io.IOException;

import static java.lang.String.format;

public class FireboltAccountRetriever<T> extends FireboltClient {
    private static final String URL = "https://api.dev.firebolt.io/web/v3/account/%s/%s";
    private final String path;
    private final Class<T> type;

    public FireboltAccountRetriever(OkHttpClient httpClient, FireboltConnection connection, String customDrivers, String customClients, ObjectMapper objectMapper, String path, Class<T> type) {
        super(httpClient, connection, customDrivers, customClients, objectMapper);
        this.path = path;
        this.type = type;
    }

    public T retrieve(String accessToken, String accountName) throws FireboltException {
        try {
            return getResource(format(URL, accountName, path), accessToken, type);
        } catch (IOException e) {
            throw new FireboltException(String.format("Failed to get %s url for account %s", path, accountName), e);
        }
    }
}
