package com.firebolt.jdbc.client.authentication;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

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
}
