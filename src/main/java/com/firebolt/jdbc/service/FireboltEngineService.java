package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

@RequiredArgsConstructor
@CustomLog
public class FireboltEngineService {
    private static final String ENGINE_URL = "url";
    private static final String STATUS_FIELD = "status";
    private static final String ENGINE_NAME_FIELD = "engine_name";
    private static final String RUNNING_STATUS = "running";
    private static final String ENGINE_QUERY =
            "SELECT engs.url, engs.attached_to, dbs.database_name, engs.status, engs.engine_name " +
            "FROM information_schema.engines as engs " +
            "LEFT JOIN information_schema.databases as dbs ON engs.attached_to = dbs.database_name " +
            "WHERE engs.engine_name = ?";
    private static final String DATABASE_QUERY = "SELECT database_name FROM information_schema.databases WHERE database_name=?";

    private static final Set<String> ENGINE_NOT_READY_STATUSES = Set.of(
            "ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_PENDING",
            "ENGINE_STATUS_PROVISIONING_FINISHED", "ENGINE_STATUS_RUNNING_REVISION_STARTING");
    private static final String ERROR_NO_ENGINE_ATTACHED = "There is no Firebolt engine running on %s attached to the database %s. To connect first make sure there is a running engine and then try again.";
    private static final String ERROR_NO_ENGINE_WITH_NAME = "There is no Firebolt engine running on %s with the name %s. To connect first make sure there is a running engine and then try again.";


    private final FireboltConnection fireboltConnection;
    private final FireboltAccountClient fireboltAccountClient;

    /**
     * Extracts the engine name from host
     *
     * @param engineHost engine host
     * @return the engine name
     */
    public String getEngineNameByHost(String engineHost) throws FireboltException {
        return Optional.ofNullable(engineHost).filter(host -> host.contains(".")).map(host -> host.split("\\.")[0])
                .map(host -> host.replace("-", "_")).orElseThrow(() -> new FireboltException(
                        format("Could not establish the engine from the host: %s", engineHost)));
    }

    public boolean doesDatabaseExist(String database) throws SQLException {
        try (PreparedStatement ps = fireboltConnection.prepareStatement(DATABASE_QUERY)) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public Engine getEngine(String engine, @Nullable String database) throws SQLException {
        if (engine == null) {
            throw new IllegalArgumentException("Cannot retrieve engine parameters because its name is null");
        }
        try (PreparedStatement ps = fireboltConnection.prepareStatement(ENGINE_QUERY)) {
            ps.setString(1, engine);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new FireboltException(format("The engine with the name %s could not be found", engine));
                }
                String status = rs.getString(STATUS_FIELD);
                if (!isEngineRunning(status)) {
                    throw new FireboltException(format("The engine with the name %s is not running. Status: %s", engine, status));
                }
                String attachedDatabase = rs.getString("attached_to");
                if (attachedDatabase == null) {
                    throw new FireboltException(format("The engine with the name %s is not attached to any database", engine));
                }
                if (database != null && !database.equals(attachedDatabase)) {
                    throw new FireboltException(format("The engine with the name %s is not attached to database %s", engine, database));
                }
                return new Engine(rs.getString(ENGINE_URL), status, rs.getString(ENGINE_NAME_FIELD), attachedDatabase, null);
            }
        }
    }

    private boolean isEngineRunning(String status) {
        return RUNNING_STATUS.equalsIgnoreCase(status);
    }


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
                .map(e -> new Engine(e.getEndpoint(), e.getCurrentStatus(), engineName, null, engineID))
                .orElseThrow(() -> new FireboltException(
                        String.format(ERROR_NO_ENGINE_WITH_NAME, connectionUrl, engineName)));
    }

    private Engine getDefaultEngine(String connectionUrl, String accountId, String database, String accessToken)
            throws FireboltException, IOException {
        FireboltDefaultDatabaseEngineResponse defaultEngine = fireboltAccountClient
                .getDefaultEngineByDatabaseName(connectionUrl, accountId, database, accessToken);

        return Optional.ofNullable(defaultEngine).map(FireboltDefaultDatabaseEngineResponse::getEngineUrl)
                .map(url -> new Engine(url, "running", null, database, null)).orElseThrow(
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

}
