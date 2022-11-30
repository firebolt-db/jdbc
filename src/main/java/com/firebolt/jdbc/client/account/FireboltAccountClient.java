package com.firebolt.jdbc.client.account;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.CustomLog;
import okhttp3.OkHttpClient;

@CustomLog
public class FireboltAccountClient extends FireboltClient {

	private static final String GET_ACCOUNT_ID_URI = "%s/iam/v2/accounts:getIdByName?accountName=%s";
	private static final String URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME = "engines:getIdByName?engine_name=";
	private static final String URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID = "engines/";
	private static final String URI_SUFFIX_DATABASE_INFO_URL = "engines:getURLByDatabaseName?databaseName=";
	private static final String URI_PREFIX_WITH_ACCOUNT_RESOURCE = "%s/core/v1/accounts/%s/%s";
	private static final String URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE = "%s/core/v1/account/%s";

	public FireboltAccountClient(OkHttpClient httpClient, ObjectMapper objectMapper,
			FireboltConnection fireboltConnection, String customDrivers, String customClients) {
		super(httpClient, fireboltConnection, customDrivers, customClients, objectMapper);
	}

	/**
	 * Returns the account
	 *
	 * @param host        the host
	 * @param account     the name of the account
	 * @param accessToken the access token
	 * @return the account
	 */
	public FireboltAccountResponse getAccount(String host, String account, String accessToken)
			throws FireboltException, IOException {
		String uri = String.format(GET_ACCOUNT_ID_URI, host, account);
		return getResource(uri, host, accessToken, FireboltAccountResponse.class);
	}

	/**
	 * Returns an engine
	 *
	 * @param host        the host
	 * @param accountId   the id of the account
	 * @param engineName  the engine name
	 * @param engineId    the engine id
	 * @param accessToken the access token
	 * @return the engine
	 */
	public FireboltEngineResponse getEngine(String host, String accountId, String engineName, String engineId,
			String accessToken) throws FireboltException, IOException {
		try {
			String uri = createAccountUri(accountId, host, URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID + engineId);
			return getResource(uri, host, accessToken, FireboltEngineResponse.class);
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
	 * Returns the default engine of the database
	 *
	 * @param host        the host
	 * @param accountId   the account id
	 * @param dbName      the name of the database
	 * @param accessToken the access token
	 * @return the default engine for the database
	 */
	public FireboltDefaultDatabaseEngineResponse getDefaultEngineByDatabaseName(String host, String accountId, String dbName,
																				String accessToken) throws FireboltException, IOException {
		String uri = createAccountUri(accountId, host, URI_SUFFIX_DATABASE_INFO_URL + dbName);
		try {
			return getResource(uri, host, accessToken, FireboltDefaultDatabaseEngineResponse.class);

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
	public FireboltEngineIdResponse getEngineId(String host, String accountId, String engineName, String accessToken)
			throws FireboltException, IOException {
		try {
			String uri = createAccountUri(accountId, host,
					URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME + engineName);
			return getResource(uri, host, accessToken, FireboltEngineIdResponse.class);
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
