package com.firebolt.jdbc.client.authentication;

import lombok.AllArgsConstructor;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.json.JSONObject;

import static java.lang.String.format;

@AllArgsConstructor
public class UsernamePasswordAuthenticationRequest implements AuthenticationRequest {
    private static final String AUTH_URL = "%s/auth/v1/login";
    private static final String USERNAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final MediaType JSON = MediaType.parse("application/json");
    private final String username;
    private final String password;
    private final String host;

    public RequestBody getRequestBody() {
        JSONObject json = new JSONObject();
        json.put(USERNAME_FIELD_NAME, username);
        json.put(PASSWORD_FIELD_NAME, password);
        return RequestBody.create(json.toString(), JSON);
    }

    @Override
    public String getUri() {
        return format(AUTH_URL, host);
    }
}
