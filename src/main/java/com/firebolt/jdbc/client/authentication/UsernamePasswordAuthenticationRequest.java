package com.firebolt.jdbc.client.authentication;

import java.util.HashMap;
import java.util.Map;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UsernamePasswordAuthenticationRequest implements AuthenticationRequest {
	private static final String AUTH_URL = "%s/auth/v1/login";
	private static final String USERNAME_FIELD_NAME = "username";
	private static final String PASSWORD_FIELD_NAME = "password";
	private String username;
	private String password;
	private String host;

	public HttpEntity getHttpEntity() throws JsonProcessingException {
		Map<String, Object> loginDetailsMap = new HashMap<>();
		loginDetailsMap.put(USERNAME_FIELD_NAME, username);
		loginDetailsMap.put(PASSWORD_FIELD_NAME, password);
		String request = new ObjectMapper().writeValueAsString(loginDetailsMap);
		return new StringEntity(request);
	}

	@Override
	public String getUri() {
		return String.format(AUTH_URL, host);
	}

}