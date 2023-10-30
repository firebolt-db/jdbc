package com.firebolt.jdbc.client.authentication;

import lombok.AllArgsConstructor;
import okhttp3.FormBody;
import okhttp3.RequestBody;

@AllArgsConstructor
public class OldServiceAccountAuthenticationRequest implements AuthenticationRequest {
    private static final String CLIENT_CREDENTIALS = "client_credentials";
    private static final String GRAND_TYPE_FIELD_NAME = "grant_type";
    private static final String CLIENT_ID_FIELD_NAME = "client_id";
    private static final String CLIENT_SECRET_FIELD_NAME = "client_secret";
    private static final String AUTH_URL = "%s/auth/v1/token";
    private String id;
    private String secret;
    private String host;

    public RequestBody getRequestBody() {
        return new FormBody.Builder().add(CLIENT_ID_FIELD_NAME, id).add(CLIENT_SECRET_FIELD_NAME, secret)
                .add(GRAND_TYPE_FIELD_NAME, CLIENT_CREDENTIALS).build();
    }

    @Override
    public String getUri() {
        return String.format(AUTH_URL, host);
    }
}