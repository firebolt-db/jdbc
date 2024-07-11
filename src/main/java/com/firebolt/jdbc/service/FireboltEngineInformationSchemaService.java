package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;


@RequiredArgsConstructor
@CustomLog
public class FireboltEngineInformationSchemaService implements FireboltEngineService {
    private static final String ENGINE_URL = "url";
    private static final String STATUS_FIELD = "status";
    private static final String ENGINE_NAME_FIELD = "engine_name";
    private static final Collection<String> RUNNING_STATUSES = Stream.of("running", "ENGINE_STATE_RUNNING").collect(toCollection(() -> new TreeSet<>(CASE_INSENSITIVE_ORDER)));
    private static final String ENGINE_QUERY =
            "SELECT engs.url, engs.attached_to, dbs.%1$s_name, engs.status, engs.engine_name " +
            "FROM information_schema.engines as engs " +
            "LEFT JOIN information_schema.%1$ss as dbs ON engs.attached_to = dbs.%1$s_name " +
            "WHERE engs.engine_name = ?";
    private static final String INVENTORY_QUERY = "SELECT %1$s_name FROM information_schema.%1$ss WHERE %1$s_name=?";

    private final FireboltConnection fireboltConnection;
    private final String dbTerm;

    public FireboltEngineInformationSchemaService(FireboltConnection fireboltConnection) throws SQLException {
        this.fireboltConnection = fireboltConnection;
        dbTerm = doesRecordExist(format(INVENTORY_QUERY, "table"), "catalogs") ? "catalog" : "database";
    }

    @Override
    public boolean doesDatabaseExist(String database) throws SQLException {
        return doesRecordExist(format(INVENTORY_QUERY, dbTerm), database);
   }

   private boolean doesRecordExist(String query, String param) throws SQLException {
       try (PreparedStatement ps = fireboltConnection.prepareStatement(query)) {
           ps.setString(1, param);
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
        try (PreparedStatement ps = fireboltConnection.prepareStatement(format(ENGINE_QUERY, dbTerm))) {
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
