package com.firebolt.jdbc.client.account;

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

import java.io.IOException;

import static java.lang.String.format;

@CustomLog
public class FireboltAccountClient extends FireboltClient {

    private static final String GET_ACCOUNT_ID_URI = "%s/iam/v2/accounts:getIdByName?accountName=%s";
    private static final String URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME = "engines:getIdByName?engine_name=";
    private static final String URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID = "engines/";
    private static final String URI_SUFFIX_DATABASE_INFO_URL = "engines:getURLByDatabaseName?databaseName=";
    private static final String URI_PREFIX_WITH_ACCOUNT_RESOURCE = "%s/core/v1/accounts/%s/%s";
    private static final String URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE = "%s/core/v1/account/%s";

    public FireboltAccountClient(OkHttpClient httpClient, FireboltConnection fireboltConnection, String customDrivers, String customClients) {
        super(httpClient, fireboltConnection, customDrivers, customClients);
    }

    /**
     * Returns the account
     *
     * @param host        the host
     * @param account     the name of the account
     * @param accessToken the access token
     * @return the account
     */
    public FireboltAccountResponse getAccount(String host, String account, String accessToken) throws FireboltException, IOException {
        String uri = format(GET_ACCOUNT_ID_URI, host, account);
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
    public FireboltEngineResponse getEngine(String host, String accountId, String engineName, String engineId, String accessToken) throws FireboltException, IOException {
        String uri = createAccountUri(accountId, host, URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID + engineId);
        return getResource(uri, host, accessToken, FireboltEngineResponse.class, format("The address of the engine with name %s and id %s could not be found", engineName, engineId));
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
    public FireboltDefaultDatabaseEngineResponse getDefaultEngineByDatabaseName(String host, String accountId, String dbName, String accessToken) throws FireboltException, IOException {
        String uri = createAccountUri(accountId, host, URI_SUFFIX_DATABASE_INFO_URL + dbName);
        return getResource(uri, host, accessToken, FireboltDefaultDatabaseEngineResponse.class, format("The database with the name %s could not be found", dbName));
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
        String uri = createAccountUri(accountId, host, URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME + engineName);
        return getResource(uri, host, accessToken, FireboltEngineIdResponse.class, format("The engine %s could not be found", engineName));
    }


    private <R> R getResource(String uri, String host, String accessToken, Class<R> responseType, String notFoundErrorMessage) throws FireboltException, IOException {
        try {
            return getResource(uri, host, accessToken, responseType);
        } catch (FireboltException exception) {
            if (exception.getType() == ExceptionType.RESOURCE_NOT_FOUND) {
                throw new FireboltException(notFoundErrorMessage, exception, ExceptionType.RESOURCE_NOT_FOUND);
            }
            throw exception;
        }
    }

    private String createAccountUri(String account, String host, String suffix) {
        return account == null || account.isEmpty() ? format(URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE, host, suffix) : format(URI_PREFIX_WITH_ACCOUNT_RESOURCE, host, account, suffix);
    }

}