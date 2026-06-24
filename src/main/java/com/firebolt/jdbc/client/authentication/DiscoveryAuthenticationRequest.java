package com.firebolt.jdbc.client.authentication;

import okhttp3.FormBody;
import okhttp3.RequestBody;

public class DiscoveryAuthenticationRequest implements AuthenticationRequest {

    private static final String AUDIENCE_FIELD_NAME = "audience";
    private static final String AUDIENCE_FIELD_VALUE = "https://api.firebolt.io";
    private static final String GRANT_TYPE_FIELD_NAME = "grant_type";
    private static final String GRANT_TYPE_FIELD_VALUE = "client_credentials";
    private static final String CLIENT_ID_FIELD_NAME = "client_id";
    private static final String CLIENT_SECRET_FIELD_NAME = "client_secret";

    private final String clientId;
    private final String clientSecret;
    private final String tokenEndpoint;

    public DiscoveryAuthenticationRequest(String clientId, String clientSecret, String tokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tokenEndpoint = tokenEndpoint;
    }

    @Override
    public RequestBody getRequestBody() {
        return new FormBody.Builder()
                .add(AUDIENCE_FIELD_NAME, AUDIENCE_FIELD_VALUE)
                .add(GRANT_TYPE_FIELD_NAME, GRANT_TYPE_FIELD_VALUE)
                .addEncoded(CLIENT_ID_FIELD_NAME, clientId)
                .addEncoded(CLIENT_SECRET_FIELD_NAME, clientSecret)
                .build();
    }

    @Override
    public String getUri() {
        return tokenEndpoint;
    }
}
