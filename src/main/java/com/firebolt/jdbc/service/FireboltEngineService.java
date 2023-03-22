package com.firebolt.jdbc.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@CustomLog
public class FireboltEngineService {
    private static final String ENGINE_URL = "engine_url";
    private static final String ENGINE_NAME = "engine_name";
    private static final String STATUS_FIELD_NAME = "status";
    private static final String DEFAULT_ENGINE_QUERY = "SELECT engs.engine_url, engs.status, engs.engine_name\n" +
            "FROM information_schema.databases AS dbs\n" +
            "INNER JOIN information_schema.engines AS engs\n" +
            "ON engs.attached_to = dbs.database_name\n" +
            "AND engs.engine_name = NULLIF(SPLIT_PART(ARRAY_FIRST(\n" +
            "        eng_name -> eng_name LIKE '%%(default)',\n" +
            "        SPLIT(',', attached_engines)\n" +
            "    ), ' ', 1), '')\n" +
            "WHERE database_name = '%s'";
    private static final String ENGINE_QUERY = "SELECT engine_url, attached_to, status FROM information_schema.engines \n" +
            "WHERE engine_name='%s'";
    private static final String RUNNING_STATUS = "running";
    private final FireboltConnection fireboltConnection;

    /**
     * Extracts the engine name from host
     *
     * @param engineHost engine host
     * @return the engine name
     */
    public String getEngineNameByHost(String engineHost) throws FireboltException {
        return Optional.ofNullable(engineHost).filter(host -> host.contains(".")).map(host -> host.split("\\.")[0])
                .map(host -> host.replace("-", "_")).orElseThrow(() -> new FireboltException(
                        String.format("Could not establish the engine from the host: %s", engineHost)));
    }

    public Engine getEngine(@Nullable String name, @Nullable String database) throws FireboltException {
        if (StringUtils.isEmpty(name)) {
            return this.getDefaultEngine(database);
        } else {
            return Engine.builder().name(name).endpoint(getEngineEndpoint(name)).build();
        }
    }

    private Engine getDefaultEngine(String database) throws FireboltException {
        try (Statement statement = this.fireboltConnection.createSystemEngineStatementStatement();
             ResultSet resultSet = statement.executeQuery(String.format(DEFAULT_ENGINE_QUERY, database))) {
            if (!resultSet.next()) {
                throw new FireboltException(String.format("The default engine for the database %s could not be found", database));
            }
            String status = resultSet.getString(STATUS_FIELD_NAME);
            if (isEngineNotRunning(status)) {
                throw new FireboltException(String.format("The default engine for the database %s is not running. Status: %s", database, status));
            }
            return Engine.builder().endpoint(resultSet.getString(ENGINE_URL)).name(resultSet.getString(ENGINE_NAME)).build();
        } catch (SQLException sqlException) {
            throw new FireboltException(String.format("Could not get default engine url for database %s", database), sqlException);
        }
    }

    private String getEngineEndpoint(String engine) throws FireboltException {
        try (Statement statement = this.fireboltConnection.createSystemEngineStatementStatement();
             ResultSet resultSet = statement.executeQuery(String.format(ENGINE_QUERY, engine))) {
            if (!resultSet.next()) {
                throw new FireboltException(String.format("The engine with the name %s could not be found", engine));
            }
            String status = resultSet.getString(STATUS_FIELD_NAME);
            if (isEngineNotRunning(status)) {
                throw new FireboltException(String.format("The engine with the name %s is not running. Status: %s", engine, status));
            }
            return resultSet.getString(ENGINE_URL);
        } catch (SQLException sqlException) {
            throw new FireboltException(String.format("Could not get engine url for engine %s", engine), sqlException);
        }
    }

    private boolean isEngineNotRunning(String status) {
        return !StringUtils.equalsIgnoreCase(RUNNING_STATUS, status);
    }


}
