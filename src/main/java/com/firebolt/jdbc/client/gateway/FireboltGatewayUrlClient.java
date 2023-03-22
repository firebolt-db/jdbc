package com.firebolt.jdbc.client.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import okhttp3.OkHttpClient;

@CustomLog
public class FireboltGatewayUrlClient extends FireboltClient {

    private static final String URL = "https://api.dev.firebolt.io/web/v3/account/%s/engineUrl";

    public FireboltGatewayUrlClient(OkHttpClient httpClient, ObjectMapper objectMapper,
                                    FireboltConnection fireboltConnection, String customDrivers, String customClients) {
        super(httpClient, fireboltConnection, customDrivers, customClients, objectMapper);
    }

    /**
     * Returns the gateway URL
     *
     * @param accessToken the access token
     * @return the account
     */
    public GatewayUrlResponse getGatewayUrl(String accessToken, String account)
            throws FireboltException {
        String url = String.format(URL, account);
        try {
            return getResource(url, accessToken, GatewayUrlResponse.class);
        } catch (FireboltException e) {
            throw e;
        } catch (Exception e) {
            throw new FireboltException(String.format("Failed to get gateway url for account %s", account), e);
        }
    }

}
