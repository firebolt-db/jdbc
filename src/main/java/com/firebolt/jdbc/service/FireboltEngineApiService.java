package com.firebolt.jdbc.service;

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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;

@RequiredArgsConstructor
@CustomLog
public class FireboltEngineApiService implements FireboltEngineService {
    private static final Set<String> ENGINE_NOT_READY_STATUSES = Set.of(
            "ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_PENDING",
            "ENGINE_STATUS_PROVISIONING_FINISHED", "ENGINE_STATUS_RUNNING_REVISION_STARTING");
    private static final String ERROR_NO_ENGINE_ATTACHED = "There is no Firebolt engine running on %s attached to the database %s. To connect first make sure there is a running engine and then try again.";
    private static final String ERROR_NO_ENGINE_WITH_NAME = "There is no Firebolt engine running on %s with the name %s. To connect first make sure there is a running engine and then try again.";


    private final FireboltAccountClient fireboltAccountClient;

    @Override
    public Engine getEngine(FireboltProperties properties) throws SQLException {
        return getEngine(properties.getHttpConnectionUrl(), properties, properties.getAccessToken());
    }

    /**
     * Returns the engine
     *
     * @param connectionUrl   the connection url
     * @param loginProperties properties to login
     * @param accessToken     the access token
     * @return the engine
     */
    private Engine getEngine(String connectionUrl, FireboltProperties loginProperties, String accessToken) throws SQLException {
        String accountId = null;
        Engine engine;
        try {
            if (loginProperties.getAccount() != null && !loginProperties.getAccount().isEmpty()) {
                accountId = getAccountId(connectionUrl, loginProperties.getAccount(), accessToken).orElse(null);
            }
            if (loginProperties.getEngine() == null || loginProperties.getEngine().isEmpty()) {
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

    private Engine getEngineWithName(String connectionUrl, String accountId, String engineName, String accessToken) throws SQLException, IOException {
        FireboltEngineIdResponse response = fireboltAccountClient.getEngineId(connectionUrl, accountId, engineName,
                accessToken);
        String engineID = ofNullable(response).map(FireboltEngineIdResponse::getEngine)
                .map(FireboltEngineIdResponse.Engine::getEngineId).orElseThrow(() -> new FireboltException(
                        "Failed to extract engine id field from the server response: the response from the server is invalid."));
        FireboltEngineResponse fireboltEngineResponse = fireboltAccountClient.getEngine(connectionUrl, accountId,
                engineName, engineID, accessToken);

        return ofNullable(fireboltEngineResponse).map(FireboltEngineResponse::getEngine)
                .filter(e -> e.getEndpoint() != null)
                .filter(e -> !e.getEndpoint().isEmpty())
                .map(e -> new Engine(e.getEndpoint(), e.getCurrentStatus(), engineName, null, engineID))
                .orElseThrow(() -> new FireboltException(
                        format(ERROR_NO_ENGINE_WITH_NAME, connectionUrl, engineName)));
    }

    private Engine getDefaultEngine(String connectionUrl, String accountId, String database, String accessToken) throws SQLException, IOException {
        FireboltDefaultDatabaseEngineResponse defaultEngine = fireboltAccountClient
                .getDefaultEngineByDatabaseName(connectionUrl, accountId, database, accessToken);

        return ofNullable(defaultEngine).map(FireboltDefaultDatabaseEngineResponse::getEngineUrl)
                .map(url -> new Engine(url, "running", null, database, null)).orElseThrow(
                        () -> new FireboltException(format(ERROR_NO_ENGINE_ATTACHED, connectionUrl, database)));
    }

    private Optional<String> getAccountId(String connectionUrl, String account, String accessToken)
            throws SQLException, IOException {
        return ofNullable(fireboltAccountClient.getAccount(connectionUrl, account, accessToken)).map(FireboltAccountResponse::getAccountId);
    }

    private void validateEngineIsNotStarting(Engine engine) throws SQLException {
        String id = engine.getId();
        String status = engine.getStatus();
        if (id != null && !id.isEmpty() && status != null && !status.isEmpty() && ENGINE_NOT_READY_STATUSES.contains(engine.getStatus())) {
            throw new FireboltException(format(
                    "The engine %s is currently starting. Please wait until the engine is on and then execute the query again.", engine.getName()));
        }
    }

}
