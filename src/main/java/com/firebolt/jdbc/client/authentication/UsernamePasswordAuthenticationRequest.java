package com.firebolt.jdbc.client.authentication;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.jdbc.client.FireboltObjectMapper;

import lombok.AllArgsConstructor;
import okhttp3.MediaType;
import okhttp3.RequestBody;

@AllArgsConstructor
public class UsernamePasswordAuthenticationRequest implements AuthenticationRequest {
    private static final String AUTH_URL = "%s/auth/v1/login";
    private static final String USERNAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private String username;
    private String password;
    private String host;

    public RequestBody getRequestBody() throws JsonProcessingException {
        Map<String, Object> loginDetailsMap = new HashMap<>();
        loginDetailsMap.put(USERNAME_FIELD_NAME, username);
        loginDetailsMap.put(PASSWORD_FIELD_NAME, password);
        return RequestBody.create(FireboltObjectMapper.getInstance().writeValueAsString(loginDetailsMap),
                MediaType.parse("application/json"));
    }

    @Override
    public String getUri() {
        return String.format(AUTH_URL, host);
    }

}