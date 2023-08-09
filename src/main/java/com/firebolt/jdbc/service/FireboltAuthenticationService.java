package com.firebolt.jdbc.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

@RequiredArgsConstructor
@CustomLog
public class FireboltAuthenticationService {

	private static final ExpiringMap<ConnectParams, FireboltConnectionTokens> tokensMap = ExpiringMap.builder()
			.variableExpiration().build();
	private static final long TOKEN_EXPIRATION_OFFSET = 5L;
	private static final long TOKEN_TTL_THRESHOLD = 60L;
	private final FireboltAuthenticationClient fireboltAuthenticationClient;

	public FireboltConnectionTokens getConnectionTokens(String host, FireboltProperties loginProperties) throws FireboltException {
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
					long durationInSeconds = getCachingDurationInSeconds(
							fireboltConnectionTokens.getExpiresInSeconds());
					tokensMap.put(connectionParams, fireboltConnectionTokens, ExpirationPolicy.CREATED,
							durationInSeconds, TimeUnit.SECONDS);
					return fireboltConnectionTokens;
				}
			}
		} catch (Exception e) {
			log.error("Failed to connect to Firebolt", e);
			if (e instanceof FireboltException
					&& StringUtils.isNotEmpty(((FireboltException) e).getErrorMessageFromServer())) {
				throw new FireboltException(String.format(
						"Failed to connect to Firebolt with the error from the server: %s, see logs for more info.",
						((FireboltException) e).getErrorMessageFromServer()), e);
			} else {
				throw new FireboltException(
						String.format("Failed to connect to Firebolt with the error: %s, see logs for more info.",
								e.getMessage()),
						e);
			}
		}
	}

	/**
	 * To avoid returning tokens that are about to expire, we store them
	 * {@link #TOKEN_EXPIRATION_OFFSET} seconds shorter than their expiry time
	 * unless the token lives for less than {@link #TOKEN_TTL_THRESHOLD} seconds.
	 */
	private long getCachingDurationInSeconds(long expireInSeconds) {
		return expireInSeconds > TOKEN_TTL_THRESHOLD ? expireInSeconds - TOKEN_EXPIRATION_OFFSET : expireInSeconds;
	}

	/**
	 * Removes connection tokens from the cache.
	 * 
	 * @param host            host
	 * @param loginProperties the login properties linked to the tokens
	 */
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
