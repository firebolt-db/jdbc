package com.firebolt.jdbc.client.authentication;

import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

@CustomLog
public abstract class FireboltAuthenticationClient extends FireboltClient {

	protected FireboltAuthenticationClient(OkHttpClient httpClient,
										   FireboltConnection connection, String customDrivers, String customClients) {
		super(httpClient, connection, customDrivers, customClients);
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
		AuthenticationRequest authenticationRequest = getAuthenticationRequest(user, password, host, environment);
		String uri = authenticationRequest.getUri();
		log.debug("Creating connection with url {}", uri);
		Request request = createPostRequest(uri, null, authenticationRequest.getRequestBody(), null);
		try (Response response = execute(request, host)) {
			String responseString = getResponseAsString(response);
			FireboltConnectionTokens authenticationTokens = jsonToObject(responseString, FireboltConnectionTokens.class);
			log.info("Successfully fetched connection token");
			logToken(authenticationTokens);
			return authenticationTokens;
		}
	}

	private void logToken(FireboltConnectionTokens connectionTokens) {
		logIfPresent(connectionTokens.getAccessToken(), "Retrieved access_token");
		if (connectionTokens.getExpiresInSeconds() >=- 0) {
			log.debug("Retrieved expires_in");
		}
	}

	private void logIfPresent(String token, String message) {
		if (token != null && !token.isEmpty()) {
			log.debug(message);
		}
	}

	protected abstract AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment);
}
