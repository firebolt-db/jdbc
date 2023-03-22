package com.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

@CustomLog
public class FireboltAuthenticationClient extends FireboltClient {

	public FireboltAuthenticationClient(OkHttpClient httpClient, ObjectMapper objectMapper,
			FireboltConnection connection, String customDrivers, String customClients) {
		super(httpClient, objectMapper, connection, customDrivers, customClients);
	}

	/**
	 * Sends POST to obtain connection tokens
	 *
	 * @param host     the host
	 * @param user     the username
	 * @param password the password
	 * @param environment the environment
	 * @return the connection tokens
	 */
	public FireboltConnectionTokens postConnectionTokens(String host, String user, String password, String environment)
			throws IOException, FireboltException {
		AuthenticationRequest authenticationRequest = new ServiceAccountAuthenticationRequest(user, password, environment);
		String uri = authenticationRequest.getUri();
		log.debug("Creating connection with url {}", uri);
		Request request = this.createPostRequest(uri, authenticationRequest.getRequestBody());
		try (Response response = this.execute(request, host)) {
			String responseString = getResponseAsString(response);
			FireboltAuthenticationResponse authenticationResponse = objectMapper.readValue(responseString,
					FireboltAuthenticationResponse.class);
			FireboltConnectionTokens authenticationTokens = FireboltConnectionTokens.builder()
					.accessToken(authenticationResponse.getAccessToken())
					.refreshToken(authenticationResponse.getRefreshToken())
					.expiresInSeconds(authenticationResponse.getExpiresIn()).build();
			log.info("Successfully fetched connection token");
			logToken(authenticationResponse);
			return authenticationTokens;
		}
	}

	private void logToken(FireboltAuthenticationResponse connectionTokens) {
		logIfPresent(connectionTokens.getAccessToken(), "Retrieved access_token");
		logIfPresent(connectionTokens.getRefreshToken(), "Retrieved refresh_token");
		if (connectionTokens.getExpiresIn() >=- 0) {
			log.debug("Retrieved expires_in");
		}
	}

	private void logIfPresent(String token, String message) {
		if (token != null && !token.isEmpty()) {
			log.debug(message);
		}
	}
}
