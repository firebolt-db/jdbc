package com.firebolt.jdbc.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;

import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

@RequiredArgsConstructor
@Slf4j
public class FireboltAuthenticationService {

	private static final ExpiringMap<ConnectParams, FireboltConnectionTokens> tokensMap = ExpiringMap.builder()
			.variableExpiration().build();
	private final FireboltAuthenticationClient fireboltAuthenticationClient;

	public FireboltConnectionTokens getConnectionTokens(String host, FireboltProperties loginProperties)
			throws FireboltException {
		try {
			ConnectParams connectionParams = new ConnectParams(host, loginProperties.getUser(),
					loginProperties.getPassword());
			synchronized (this) {
				FireboltConnectionTokens foundToken = tokensMap.get(connectionParams);
				if (foundToken != null) {
					log.debug("Using the token of {} from the cache", host);
					return foundToken;
				} else {
					FireboltConnectionTokens fireboltConnectionTokens = fireboltAuthenticationClient
							.postConnectionTokens(host, loginProperties.getUser(), loginProperties.getPassword());
					tokensMap.put(connectionParams, fireboltConnectionTokens, ExpirationPolicy.CREATED,
							fireboltConnectionTokens.getExpiresInSeconds(), TimeUnit.SECONDS);
					return fireboltConnectionTokens;
				}
			}
		} catch (Exception e) {
			throw new FireboltException("Could not get connection tokens", e);
		}
	}

	public void removeConnectionTokens(String host, FireboltProperties loginProperties) throws FireboltException {
		try {
			log.debug("Removing connection token for host {}", host);
			ConnectParams connectionParams = new ConnectParams(host, loginProperties.getUser(),
					loginProperties.getPassword());
			tokensMap.remove(connectionParams);
		} catch (NoSuchAlgorithmException e) {
			throw new FireboltException("Could not remove connection tokens", e);
		}
	}

	@EqualsAndHashCode
	private static class ConnectParams {
		public final String fireboltHost;
		public final String credentialsHash;

		public ConnectParams(String fireboltHost, String user, String password) throws NoSuchAlgorithmException {
			this.fireboltHost = fireboltHost;
			MessageDigest sha256Instance = MessageDigest.getInstance("SHA-256");
			Optional.ofNullable(user).map(String::getBytes).ifPresent(sha256Instance::update);
			Optional.ofNullable(password).map(String::getBytes).ifPresent(sha256Instance::update);
			this.credentialsHash = new String(Hex.encodeHex(sha256Instance.digest()));
		}
	}
}
