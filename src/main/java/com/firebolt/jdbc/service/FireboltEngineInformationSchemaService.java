package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

import java.net.URI;
import java.net.URISyntaxException;

@CustomLog
public class FireboltEngineInformationSchemaService implements FireboltEngineService {
    private static final String ENGINE_URL = "url";
    private static final String STATUS_FIELD = "status";
    private static final String ENGINE_NAME_FIELD = "engine_name";
    private static final Set<String> RUNNING_STATUS = Set.of("ENGINE_STATE_RUNNING", "RUNNING");
    
    
    private static final String ENGINE_QUERY =
            "SELECT engs.url, " + 
                   "engs.default_database as default_db_name, " +
                   "catalogs.catalog_name, "+
                   "engs.status, " + 
                   "engs.engine_name, " + 
                   "dbs.database_name as attached_to_database_name " +
            "FROM information_schema.engines as engs " +
            "LEFT OUTER JOIN information_schema.catalogs as catalogs ON engs.default_database = catalogs.catalog_name " +
            "LEFT OUTER JOIN information_schema.databases as dbs ON engs.attached_to = dbs.database_name " +
            "WHERE engs.engine_name = ?";

    private static final String DATABASE_QUERY_DATABASE_TABLE = "SELECT database_name FROM information_schema.databases WHERE database_name=?";
    private static final String DATABASE_QUERY_CATALOG_TABLE = "SELECT catalog_name FROM information_schema.catalogs WHERE catalog_name=?";

    private final FireboltConnection fireboltConnection;

    public FireboltEngineInformationSchemaService(FireboltConnection fireboltConnection) {
        this.fireboltConnection = fireboltConnection;
    }

    @Override
    public boolean doesDatabaseExist(String database) throws SQLException {
        try (PreparedStatement ps = fireboltConnection.prepareStatement(DATABASE_QUERY_DATABASE_TABLE)) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasNext = rs.next();
                if (hasNext) {
                    return hasNext;
                }
            }
        } 

        try (PreparedStatement ps = fireboltConnection.prepareStatement(DATABASE_QUERY_CATALOG_TABLE)) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    @Override
    public Engine getEngine(FireboltProperties properties) throws SQLException {
        return getEngine(properties.getEngine(), properties.getDatabase());
    }

    private Engine getEngine(String engine, @Nullable String database) throws SQLException {
        if (engine == null) {
            throw new IllegalArgumentException("Cannot retrieve engine parameters because its name is null");
        }

        String rawUrl = "";
        String attachedDatabase = null;
        String engineName = "";
        String status = "";

        try (PreparedStatement ps = fireboltConnection.prepareStatement(ENGINE_QUERY)) {
            ps.setString(1, engine);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new FireboltException(format("The engine with the name %s could not be found", engine));
                }
                status = rs.getString(STATUS_FIELD);
                engineName = rs.getString(ENGINE_NAME_FIELD);
                attachedDatabase = rs.getString("attached_to_database_name");
                if (attachedDatabase == null) {
                    attachedDatabase = rs.getString("default_db_name");
                }
                rawUrl = rs.getString(ENGINE_URL);
            }

            if (!isEngineRunning(status)) {
                throw new FireboltException(format("The engine with the name %s is not running. Status: %s", engine, status));
            }

            // If the engine url has query parameters, we want to keep them and send on requests. 
            String hostUrl = rawUrl;
            URI uri = new URI(rawUrl);

            if (rawUrl.indexOf("?") != -1) {
                hostUrl = rawUrl.substring(0, rawUrl.indexOf("?"));
            }

            List<ImmutablePair<String, String>> queryParams = new ArrayList<>();
            String query = uri.getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] pair = param.split("=");
                    queryParams.add(new ImmutablePair<String, String>(pair[0], pair[1]));
                }
            }

            if (attachedDatabase == null) {
                throw new FireboltException(format("The engine with the name %s is not attached to any database", engine));
            }

            return new Engine(hostUrl, status, engineName, attachedDatabase, null, queryParams);
        } catch (URISyntaxException e) {
            throw new FireboltException(format("The engine with the name %s has an invalid url", engine), e);
        }
    }

    private boolean isEngineRunning(String status) {
        return RUNNING_STATUS.contains(status.toUpperCase());
    }
}
