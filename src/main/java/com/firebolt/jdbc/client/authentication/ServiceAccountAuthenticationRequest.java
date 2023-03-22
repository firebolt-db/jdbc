package com.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import okhttp3.FormBody;
import okhttp3.RequestBody;

@AllArgsConstructor
public class ServiceAccountAuthenticationRequest implements AuthenticationRequest {

    private static final String AUDIENCE_FIELD_NAME = "audience";
    private static final String GRAND_TYPE_FIELD_NAME = "grant_type";
    private static final String GRAND_TYPE_FIELD_VALUE = "client_credentials";
    private static final String CLIENT_ID_FIELD_NAME = "client_id";
    private static final String CLIENT_SECRET_FIELD_NAME = "client_secret";
    private static final String AUTH_URL = "https://id.%s.firebolt.io/oauth/token";

    private final String clientId;
    private final String clientSecret;
    private final String environment;

    @Override
    public RequestBody getRequestBody() throws JsonProcessingException {
        return new FormBody.Builder().add(AUDIENCE_FIELD_NAME, String.format("https://%s-firebolt-v2.us.auth0.com/api/v2/", environment))
                .add(GRAND_TYPE_FIELD_NAME, GRAND_TYPE_FIELD_VALUE)
                .add(CLIENT_ID_FIELD_NAME, clientId)
                .add(CLIENT_SECRET_FIELD_NAME, clientSecret).build();
    }

    @Override
    public String getUri() {
        return String.format(AUTH_URL, environment);
    }
}
