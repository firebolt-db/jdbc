package com.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import lombok.AllArgsConstructor;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.util.Map;

import static java.lang.String.format;

@AllArgsConstructor
public class UsernamePasswordAuthenticationRequest implements AuthenticationRequest {
    private static final String AUTH_URL = "%s/auth/v1/login";
    private static final String USERNAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final MediaType JSON = MediaType.parse("application/json");
    private final ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
    private final String username;
    private final String password;
    private final String host;

    public RequestBody getRequestBody() throws JsonProcessingException {
        Map<String, String> loginDetailsMap = Map.of(USERNAME_FIELD_NAME, username, PASSWORD_FIELD_NAME, password);
        return RequestBody.create(objectMapper.writeValueAsString(loginDetailsMap), JSON);
    }

    @Override
    public String getUri() {
        return format(AUTH_URL, host);
    }
}
