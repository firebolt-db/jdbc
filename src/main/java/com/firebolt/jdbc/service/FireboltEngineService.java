package com.firebolt.jdbc.service;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static java.lang.String.format;

@RequiredArgsConstructor
@CustomLog
public class FireboltEngineService {
    private static final String ENGINE_URL = "url";
    private static final String ENGINE_NAME = "engine_name";
    private static final String STATUS_FIELD_NAME = "status";
    private static final String DEFAULT_ENGINE_QUERY = "SELECT engs.url, engs.status, engs.engine_name\n" +
            "FROM information_schema.databases AS dbs\n" +
            "INNER JOIN information_schema.engines AS engs\n" +
            "ON engs.attached_to = dbs.database_name\n" +
            "AND engs.engine_name = NULLIF(SPLIT_PART(ARRAY_FIRST(\n" +
            "        eng_name -> eng_name LIKE '%%(default)',\n" +
            "        SPLIT(',', attached_engines)\n" +
            "    ), ' ', 1), '')\n" +
            "WHERE database_name = ?";
    private static final String ENGINE_QUERY = "SELECT url, status FROM information_schema.engines WHERE engine_name=?";
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
                        format("Could not establish the engine from the host: %s", engineHost)));
    }

    public Engine getEngine(@Nullable String name, @Nullable String database) throws FireboltException {
        if (StringUtils.isEmpty(name)) {
            return getDefaultEngine(database);
        } else {
            return Engine.builder().name(name).endpoint(getEngineEndpoint(name)).build();
        }
    }

    private Engine getDefaultEngine(String database) throws FireboltException {
        return getRowValue(DEFAULT_ENGINE_QUERY, database, rs -> Engine.builder().endpoint(rs.getString(ENGINE_URL)).name(rs.getString(ENGINE_NAME)).build(),
                "The default engine for the database %s could not be found",
                "The default engine for the database %s is not running. Status: %s",
                "Could not get default engine url for database %s");
    }

    private String getEngineEndpoint(String engine) throws FireboltException {
        return getRowValue(ENGINE_QUERY, engine, rs -> rs.getString(ENGINE_URL),
                "The engine with the name %s could not be found",
                "The engine with the name %s is not running. Status: %s",
                "Could not get engine url for engine %s");
    }

    private <T> T getRowValue(String query, String queryArg, CheckedFunction<ResultSet, T> valueExtractor, String noDataMessage, String engineNotRunningMessage, String sqlFailureMessage) throws FireboltException {
        try (PreparedStatement ps = fireboltConnection.prepareStatement(query)) {
            ps.setString(1, queryArg);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new FireboltException(format(noDataMessage, queryArg));
                }
                String status = rs.getString(STATUS_FIELD_NAME);
                if (isEngineNotRunning(status)) {
                    throw new FireboltException(format(engineNotRunningMessage, queryArg, status));
                }
                return valueExtractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new FireboltException(format(sqlFailureMessage, queryArg), e);
        }
    }

    private boolean isEngineNotRunning(String status) {
        return !RUNNING_STATUS.equalsIgnoreCase(status);
    }
}
