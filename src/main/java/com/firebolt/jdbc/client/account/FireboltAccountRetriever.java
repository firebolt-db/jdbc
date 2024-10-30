package com.firebolt.jdbc.client.account;

import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.CacheListener;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class FireboltAccountRetriever<T> extends FireboltClient implements CacheListener {
    private static final String URL = "https://%s/web/v3/account/%s/engineUrl";
    private final String host;
    private final Class<T> type;
    private static final Map<String, Object> valueCache = new ConcurrentHashMap<>();

    public FireboltAccountRetriever(OkHttpClient httpClient, FireboltConnection connection, String customDrivers, String customClients, String host, Class<T> type) {
        super(httpClient, connection, customDrivers, customClients);
        this.host = host;
        this.type = type;
    }

    public T retrieve(String accessToken, String accountName) throws SQLException {
        try {
            String url = format(URL, host, accountName);
            @SuppressWarnings("unchecked")
            T value = (T)valueCache.get(url);
            if (value == null) {
                value = getResource(url, accessToken, type);
                valueCache.put(url, value);
            }
            return value;
        } catch (IOException e) {
            throw new FireboltException(String.format("Failed to get engine url for account %s: %s", accountName, e.getMessage()), e);
        }
    }

    @Override
    protected void validateResponse(String host, int statusCode, String errorMessageFromServer) throws SQLException {
        if (statusCode == HTTP_NOT_FOUND) {
            String[] fragments = host.split("/");
            // Second to last because th last element presents action and the second to last is the account name
            // The validation of the array length is done "just in case" to be safe for future, probably wrong, modifications
            // because the last thing we want is to fail on ArrayIndexOutOfBounds when creating error message.
            String account = fragments.length > 1 ? fragments[fragments.length - 2] : "N/A";
            throw new FireboltException(
                    format("Account '%s' does not exist in this organization or is not authorized. " +
                           "Please verify the account name and make sure your service account has the correct RBAC permissions and is linked to a user.", account),
                    statusCode, errorMessageFromServer);
        }
    }

    @Override
    public void cleanup() {
        valueCache.clear();
    }
}
