package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

@RequiredArgsConstructor
public class FireboltEngineInformationSchemaService implements FireboltEngineService {
    private static final String ENGINE_URL = "url";
    private static final String STATUS_FIELD = "status";
    private static final String ENGINE_NAME_FIELD = "engine_name";
    private static final Collection<String> RUNNING_STATUSES = Stream.of("running", "ENGINE_STATE_RUNNING").collect(toCollection(() -> new TreeSet<>(CASE_INSENSITIVE_ORDER)));
    private static final String ENGINE_QUERY =
            "SELECT engs.url, engs.attached_to, dbs.database_name, engs.status, engs.engine_name " +
            "FROM information_schema.engines as engs " +
            "LEFT JOIN information_schema.databases as dbs ON engs.attached_to = dbs.database_name " +
            "WHERE engs.engine_name = ?";
    private static final String DATABASE_QUERY = "SELECT database_name FROM information_schema.databases WHERE database_name=?";

    private final FireboltConnection fireboltConnection;

    @Override
    public boolean doesDatabaseExist(String database) throws SQLException {
        try (PreparedStatement ps = fireboltConnection.prepareStatement(DATABASE_QUERY)) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
   }

    @Override
    public Engine getEngine(FireboltProperties properties) throws SQLException {
        String engine = properties.getEngine();
        String database = properties.getDatabase();
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
                String engineHost = properties.processEngineUrl(rs.getString(ENGINE_URL));
                return new Engine(engineHost, status, rs.getString(ENGINE_NAME_FIELD), attachedDatabase, null);
            }
        }
    }

    private boolean isEngineRunning(String status) {
        return RUNNING_STATUSES.contains(status);
    }
}
