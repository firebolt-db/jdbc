package com.firebolt.jdbc.client.account;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDatabaseResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltAccountClient extends FireboltClient {

	private static final String GET_ACCOUNT_ID_URI = "%s/iam/v2/accounts:getIdByName?accountName=%s";

	private static final String URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME = "engines:getIdByName?engine_name=";

	private static final String URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID = "engines/";
	private static final String URI_SUFFIX_DATABASE_INFO_URL = "engines:getURLByDatabaseName?databaseName=";

	private static final String URI_PREFIX_WITH_ACCOUNT_RESOURCE = "%s/core/v1/accounts/%s/%s";

	private static final String URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE = "%s/core/v1/account/%s";

	private static final String ERROR_NO_RUNNING_ENGINE_SUFFIX = ". To connect first make sure there is a running engine and then try again.";
	private static final String ERROR_NO_RUNNING_ENGINE_PREFIX = "There is no running Firebolt engine running on ";

	public FireboltAccountClient(CloseableHttpClient httpClient, ObjectMapper objectMapper,
			FireboltConnection fireboltConnection, String customDrivers, String customClients) {
		super(httpClient, fireboltConnection, customDrivers, customClients, objectMapper);
	}

	/**
	 * Returns the accountId
	 * 
	 * @param host        the host
	 * @param account     the name of the account
	 * @param accessToken the access token
	 * @return the account id
	 */
	public Optional<String> getAccountId(String host, String account, String accessToken)
			throws FireboltException, IOException, ParseException {
		String uri = String.format(GET_ACCOUNT_ID_URI, host, account);
		return Optional.ofNullable(getResource(uri, host, accessToken, FireboltAccountResponse.class))
				.map(FireboltAccountResponse::getAccountId);
	}

	/**
	 * Returns the accountId
	 * 
	 * @param host        the host
	 * @param accountId   the id of the account
	 * @param engineName  the engine name
	 * @param engineId    the engine id
	 * @param accessToken the access token
	 * @return the account id
	 */
	public String getEngineAddress(String host, String accountId, String engineName, String engineId,
			String accessToken) throws FireboltException, IOException, ParseException {
		try {
			String uri = createAccountUri(accountId, host, URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID + engineId);
			FireboltEngineResponse response = getResource(uri, host, accessToken, FireboltEngineResponse.class);
			return Optional.ofNullable(response).map(FireboltEngineResponse::getEngine)
					.map(FireboltEngineResponse.Engine::getEndpoint)
					.orElseThrow(() -> new FireboltException(ERROR_NO_RUNNING_ENGINE_PREFIX + host + " attached to "
							+ engineName + ERROR_NO_RUNNING_ENGINE_SUFFIX));
		} catch (FireboltException exception) {
			if (exception.getType() == ExceptionType.RESOURCE_NOT_FOUND) {
				throw new FireboltException(
						String.format("The address of the engine with name %s and id %s could not be found", engineName,
								engineId),
						exception, ExceptionType.RESOURCE_NOT_FOUND);
			} else {
				throw exception;
			}
		}
	}

	/**
	 * Returns the default engine address of the database
	 * 
	 * @param host        the host
	 * @param accountId   the account id
	 * @param dbName      the name of the database
	 * @param accessToken the access token
	 * @return the default engine address of the database
	 */
	public String getDbDefaultEngineAddress(String host, String accountId, String dbName, String accessToken)
			throws FireboltException, IOException, ParseException {
		String uri = createAccountUri(accountId, host, URI_SUFFIX_DATABASE_INFO_URL + dbName);
		try {
			FireboltDatabaseResponse response = getResource(uri, host, accessToken, FireboltDatabaseResponse.class);
			return Optional.ofNullable(response).map(FireboltDatabaseResponse::getEngineUrl)
					.orElseThrow(() -> new FireboltException(ERROR_NO_RUNNING_ENGINE_PREFIX + host + " attached to "
							+ dbName + ERROR_NO_RUNNING_ENGINE_SUFFIX));
		} catch (FireboltException exception) {
			if (exception.getType() == ExceptionType.RESOURCE_NOT_FOUND) {
				throw new FireboltException(String.format("The database with the name %s could not be found", dbName),
						exception, ExceptionType.RESOURCE_NOT_FOUND);
			} else {
				throw exception;
			}
		}
	}

	/**
	 * Returns the engine id
	 * 
	 * @param host        the host
	 * @param accountId   the account id
	 * @param engineName  the name of the engine
	 * @param accessToken the access token
	 * @return the engine id
	 */
	public String getEngineId(String host, String accountId, String engineName, String accessToken)
			throws FireboltException, IOException, ParseException {
		try {
			String uri = createAccountUri(accountId, host,
					URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME + engineName);
			FireboltEngineIdResponse response = getResource(uri, host, accessToken, FireboltEngineIdResponse.class);
			return Optional.ofNullable(response).map(FireboltEngineIdResponse::getEngine)
					.map(FireboltEngineIdResponse.Engine::getEngineId)
					.orElseThrow(() -> new FireboltException(ERROR_NO_RUNNING_ENGINE_PREFIX + host + " with the name "
							+ engineName + ERROR_NO_RUNNING_ENGINE_SUFFIX));
		} catch (FireboltException exception) {
			if (exception.getType() == ExceptionType.RESOURCE_NOT_FOUND) {
				throw new FireboltException(String.format("The engine %s could not be found", engineName), exception,
						ExceptionType.RESOURCE_NOT_FOUND);
			} else {
				throw exception;
			}
		}
	}

	private String createAccountUri(String account, String host, String suffix) {
		if (StringUtils.isNotEmpty(account))
			return String.format(URI_PREFIX_WITH_ACCOUNT_RESOURCE, host, account, suffix);
		else
			return String.format(URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE, host, suffix);
	}
}
