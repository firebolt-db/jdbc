package com.firebolt.jdbc.client.authentication;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.exception.FireboltException;
import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltAuthenticationClient extends FireboltClient {
	private static final String USERNAME = "username";
	private static final String PASSWORD = "password";
	private static final String AUTH_URL = "%s/auth/v1/login";

	public FireboltAuthenticationClient(CloseableHttpClient httpClient, ObjectMapper objectMapper,
			FireboltConnection connection, String customDrivers, String customClients) {
		super(httpClient, connection, customDrivers, customClients, objectMapper);
	}

	/**
	 * Sends POST to obtain connection tokens
	 * 
	 * @param host     the host
	 * @param user     the username
	 * @param password the password
	 * @return the connection tokens
	 */
	public FireboltConnectionTokens postConnectionTokens(String host, String user, String password)
			throws IOException, ParseException, FireboltException {
		String connectUrl = String.format(AUTH_URL, host);
		log.debug("Creating connection with url {}", connectUrl);
		HttpPost post = this.createPostRequest(connectUrl);
		post.addHeader(HEADER_USER_AGENT, this.getHeaderUserAgentValue());
		post.setEntity(new StringEntity(createLoginRequest(user, password)));
		try (CloseableHttpResponse response = this.execute(post, host)) {
			String responseStr = EntityUtils.toString(response.getEntity());
			FireboltAuthenticationResponse authenticationResponse = objectMapper.readValue(responseStr,
					FireboltAuthenticationResponse.class);
			FireboltConnectionTokens authenticationTokens = FireboltConnectionTokens.builder()
					.accessToken(authenticationResponse.getAccessToken())
					.refreshToken(authenticationResponse.getRefreshToken())
					.expiresInSeconds(authenticationResponse.getExpiresIn()).build();
			log.info("Successfully fetched connection tokens");
			logToken(authenticationResponse);
			return authenticationTokens;
		}
	}

	private void logToken(FireboltAuthenticationResponse connectionTokens) {

		if (!StringUtils.isEmpty(connectionTokens.getAccessToken())) {
			log.debug("Retrieved access_token");
		}

		if (!StringUtils.isEmpty(connectionTokens.getRefreshToken())) {
			log.debug("Retrieved refresh_token");
		}
		if (0 <= connectionTokens.getExpiresIn()) {
			log.debug("Retrieved expires_in");
		}
	}

	private String createLoginRequest(String username, String password) throws JsonProcessingException {
		return new ObjectMapper().writeValueAsString(ImmutableMap.of(USERNAME, username, PASSWORD, password));
	}
}
