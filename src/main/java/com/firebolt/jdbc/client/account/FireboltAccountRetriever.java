package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.OkHttpClient;

import java.io.IOException;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class FireboltAccountRetriever<T> extends FireboltClient {
    private static final String URL = "https://%s/web/v3/account/%s/%s";
    private final String host;
    private final String path;
    private final Class<T> type;

    @SuppressWarnings("java:S107") //Number of parameters (8) > max (7). This is the price of the immutability
    public FireboltAccountRetriever(OkHttpClient httpClient, ObjectMapper objectMapper, FireboltConnection connection, String customDrivers, String customClients, String host, String path, Class<T> type) {
        super(httpClient, objectMapper, connection, customDrivers, customClients);
        this.host = host;
        this.path = path;
        this.type = type;
    }

    public T retrieve(String accessToken, String accountName) throws FireboltException {
        try {
            return getResource(format(URL, host, accountName, path), accessToken, type);
        } catch (IOException e) {
            throw new FireboltException(String.format("Failed to get %s url for account %s", path, accountName), e);
        }
    }

    @Override
    protected void validateResponse(String host, int statusCode, String errorMessageFromServer) throws FireboltException {
        if (statusCode == HTTP_NOT_FOUND) {
            String[] fragments = host.split("/");
            // Second to last because th last element presents action and the second to last is the account name
            // The validation of the array length is done "just in case" to be safe for future, probably wrong, modifications
            // because the last think we want is to fail on ArrayIndexOutOfBounds when creating error message.
            String account = fragments.length > 1 ? fragments[fragments.length - 2] : "N/A";
            throw new FireboltException(
                    format("Account '%s' does not exist in this organization or is not authorized. " +
                           "Please verify the account name, organization, check that your service account has the correct RBAC permission and is linked to a user.", account),
                    statusCode, errorMessageFromServer);
        }
    }
}
