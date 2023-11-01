package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import net.jodah.expiringmap.ExpiringMap;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.jodah.expiringmap.ExpirationPolicy.CREATED;

@RequiredArgsConstructor
@CustomLog
public class FireboltAuthenticationService {

	private static final ExpiringMap<ConnectParams, FireboltConnectionTokens> tokensMap = ExpiringMap.builder()
			.variableExpiration().build();
	private static final long TOKEN_EXPIRATION_OFFSET = 5L;
	private static final long TOKEN_TTL_THRESHOLD = 60L;
	private static final String ERROR_MESSAGE = "Failed to connect to Firebolt with the error: %s, see logs for more info.";
	private static final String ERROR_MESSAGE_FROM_SERVER = "Failed to connect to Firebolt with the error from the server: %s, see logs for more info.";
	private final FireboltAuthenticationClient fireboltAuthenticationClient;

	public FireboltConnectionTokens getConnectionTokens(String host, FireboltProperties loginProperties) throws FireboltException {
		try {
			ConnectParams connectionParams = new ConnectParams(host, loginProperties.getPrincipal(), loginProperties.getSecret());
			synchronized (this) {
				FireboltConnectionTokens foundToken = tokensMap.get(connectionParams);
				if (foundToken != null) {
					log.debug("Using the token of {} from the cache", host);
					return foundToken;
				}
				FireboltConnectionTokens fireboltConnectionTokens = fireboltAuthenticationClient
						.postConnectionTokens(host, loginProperties.getPrincipal(), loginProperties.getSecret(), loginProperties.getEnvironment());
				long durationInSeconds = getCachingDurationInSeconds(fireboltConnectionTokens.getExpiresInSeconds());
				tokensMap.put(connectionParams, fireboltConnectionTokens, CREATED, durationInSeconds, SECONDS);
				return fireboltConnectionTokens;
			}
		} catch (FireboltException e) {
			log.error("Failed to connect to Firebolt", e);
			String msg = ofNullable(e.getErrorMessageFromServer()).map(m -> format(ERROR_MESSAGE_FROM_SERVER, m)).orElse(format(ERROR_MESSAGE, e.getMessage()));
			throw new FireboltException(msg, e);
		} catch (Exception e) {
			log.error("Failed to connect to Firebolt", e);
			throw new FireboltException(format(ERROR_MESSAGE, e.getMessage()), e);
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
			ConnectParams connectionParams = new ConnectParams(host, loginProperties.getPrincipal(), loginProperties.getSecret());
			tokensMap.remove(connectionParams);
		} catch (NoSuchAlgorithmException e) {
			throw new FireboltException("Could not remove connection tokens", e);
		}
	}

	@EqualsAndHashCode
	private static class ConnectParams {
		public final String fireboltHost;
		public final String credentialsHash;

		public ConnectParams(String fireboltHost, String principal, String secret) throws NoSuchAlgorithmException {
			this.fireboltHost = fireboltHost;
			MessageDigest sha256Instance = MessageDigest.getInstance("SHA-256");
			ofNullable(principal).map(String::getBytes).ifPresent(sha256Instance::update);
			ofNullable(secret).map(String::getBytes).ifPresent(sha256Instance::update);
			this.credentialsHash = DatatypeConverter.printHexBinary(sha256Instance.digest());
		}
	}
}
