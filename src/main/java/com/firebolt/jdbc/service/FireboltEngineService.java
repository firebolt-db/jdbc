package com.firebolt.jdbc.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@CustomLog
public class FireboltEngineService {
	private static final Set<String> ENGINE_NOT_READY_STATUSES = new HashSet<>(
			Arrays.asList("ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_PENDING",
					"ENGINE_STATUS_PROVISIONING_FINISHED", "ENGINE_STATUS_RUNNING_REVISION_STARTING"));
	private static final String ERROR_NO_ENGINE_ATTACHED = "There is no Firebolt engine running on %s attached to the database %s. To connect first make sure there is a running engine and then try again.";
	private static final String ERROR_NO_ENGINE_WITH_NAME = "There is no Firebolt engine running on %s with the name %s. To connect first make sure there is a running engine and then try again.";
	private final FireboltAccountClient fireboltAccountClient;

	/**
	 * Returns the engine
	 *
	 * @param connectionUrl   the connection url
	 * @param loginProperties properties to login
	 * @param accessToken     the access token
	 * @return the engine
	 */
	public Engine getEngine(String connectionUrl, FireboltProperties loginProperties, String accessToken)
			throws FireboltException {
		String accountId = null;
		Engine engine;
		try {
			if (StringUtils.isNotEmpty(loginProperties.getAccount())) {
				accountId = getAccountId(connectionUrl, loginProperties.getAccount(), accessToken).orElse(null);
			}
			if (StringUtils.isEmpty(loginProperties.getEngine())) {
				engine = getDefaultEngine(connectionUrl, accountId, loginProperties.getDatabase(), accessToken);
			} else {
				engine = getEngineWithName(connectionUrl, accountId, loginProperties.getEngine(), accessToken);
			}
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			throw new FireboltException("Failed to get engine", e);
		}
		validateEngineIsNotStarting(engine);
		return engine;
	}

	private Engine getEngineWithName(String connectionUrl, String accountId, String engineName, String accessToken)
			throws FireboltException, IOException {
		FireboltEngineIdResponse response = fireboltAccountClient.getEngineId(connectionUrl, accountId, engineName,
				accessToken);
		String engineID = Optional.ofNullable(response).map(FireboltEngineIdResponse::getEngine)
				.map(FireboltEngineIdResponse.Engine::getEngineId).orElseThrow(() -> new FireboltException(
						"Failed to extract engine id field from the server response: the response from the server is invalid."));
		FireboltEngineResponse fireboltEngineResponse = fireboltAccountClient.getEngine(connectionUrl, accountId,
				engineName, engineID, accessToken);

		return Optional.ofNullable(fireboltEngineResponse).map(FireboltEngineResponse::getEngine)
				.filter(e -> StringUtils.isNotEmpty(e.getEndpoint()))
				.map(e -> Engine.builder().endpoint(e.getEndpoint()).id(engineID).status(e.getCurrentStatus())
						.name(engineName).build())
				.orElseThrow(() -> new FireboltException(
						String.format(ERROR_NO_ENGINE_WITH_NAME, connectionUrl, engineName)));
	}

	private Engine getDefaultEngine(String connectionUrl, String accountId, String database, String accessToken)
			throws FireboltException, IOException {
		FireboltDefaultDatabaseEngineResponse defaultEngine = fireboltAccountClient
				.getDefaultEngineByDatabaseName(connectionUrl, accountId, database, accessToken);
		return Optional.ofNullable(defaultEngine).map(FireboltDefaultDatabaseEngineResponse::getEngineUrl)
				.map(url -> Engine.builder().endpoint(url).build()).orElseThrow(
						() -> new FireboltException(String.format(ERROR_NO_ENGINE_ATTACHED, connectionUrl, database)));
	}

	private Optional<String> getAccountId(String connectionUrl, String account, String accessToken)
			throws FireboltException, IOException {
		FireboltAccountResponse fireboltAccountResponse = fireboltAccountClient.getAccount(connectionUrl, account,
				accessToken);
		return Optional.ofNullable(fireboltAccountResponse).map(FireboltAccountResponse::getAccountId);
	}

	private void validateEngineIsNotStarting(Engine engine) throws FireboltException {
		if (StringUtils.isNotEmpty(engine.getId()) && StringUtils.isNotEmpty(engine.getStatus())
				&& ENGINE_NOT_READY_STATUSES.contains(engine.getStatus())) {
			throw new FireboltException(String.format(
					"The engine %s is currently starting. Please wait until the engine is on and then execute the query again.", engine.getName()));
		}
	}

	/**
	 * Extracts the engine name from host
	 *
	 * @param engineHost engine host
	 * @return the engine name
	 */
	public String getEngineNameFromHost(String engineHost) throws FireboltException {
		return Optional.ofNullable(engineHost).filter(host -> host.contains(".")).map(host -> host.split("\\.")[0])
				.map(host -> host.replace("-", "_")).orElseThrow(() -> new FireboltException(
						String.format("Could not establish the engine from the host: %s", engineHost)));
	}

}
