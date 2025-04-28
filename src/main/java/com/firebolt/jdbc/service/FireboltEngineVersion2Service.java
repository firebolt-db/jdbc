package com.firebolt.jdbc.service;

import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.ConnectionCache;
import com.firebolt.jdbc.cache.DatabaseOptions;
import com.firebolt.jdbc.cache.EngineOptions;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import static java.lang.String.format;

@CustomLog
public class FireboltEngineVersion2Service {

    private static final boolean DO_NOT_VALIDATE_CONNECTION_FLAG = false;

    private final FireboltConnection fireboltConnection;

    public FireboltEngineVersion2Service(FireboltConnection fireboltConnection) {
        this.fireboltConnection = fireboltConnection;
    }

    @SuppressWarnings("java:S2077") // Formatting SQL queries is security-sensitive - looks safe in this case
    public Engine getEngine(FireboltProperties properties, Optional<ConnectionCache> connectionCacheOptional, CacheService cacheService, CacheKey cacheKey) throws SQLException {
        try (Statement statement = fireboltConnection.createStatement()) {
            if (StringUtils.isNotBlank(properties.getDatabase())) {
                getAndSetDatabaseProperties(statement, properties.getDatabase(), connectionCacheOptional, cacheService, cacheKey);
            }
            getAndSetEngineProperties(statement, properties.getEngine(), connectionCacheOptional, cacheService, cacheKey);
        }
        // now session properties are updated with new database and engine
        FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
        return new Engine(fireboltConnection.getEndpoint(), null, sessionProperties.getEngine(), sessionProperties.getDatabase(), null);
    }

    /**
     * Once we verify a database with the backend we will cache its result and would consider the database valid in between connection establishing.
     * If the database was deleted between the time a connection cached the database and the second connection using the cached value, then this would only
     * be caught while executing the statement, not during connection time.
     *
     * @param statement - statement that will execute the verification of the database
     * @param databaseName - the name of the database to check
     */

    private void getAndSetDatabaseProperties(Statement statement, String databaseName, final Optional<ConnectionCache> connectionCacheOptional, CacheService cacheService, CacheKey cacheKey) throws SQLException {
        // if the connection cache is empty it means it is not cachable
        if (connectionCacheOptional.isEmpty()) {
            // if no caching of the result then just make the call
            statement.executeUpdate(use("DATABASE", databaseName));
        } else {
            // check the cache first
            ConnectionCache connectionCache = connectionCacheOptional.get();
            Optional<DatabaseOptions> databaseOptions = connectionCache.getDatabaseOptions(databaseName);

            if (databaseOptions.isPresent()) {
                updateDatabasePropertiesOnConnection(databaseOptions.get());
                return;
            }

            synchronized (connectionCache) {
                // make sure another thread did not already populate it
                databaseOptions = connectionCache.getDatabaseOptions(databaseName);
                if (databaseOptions.isPresent()) {
                    updateDatabasePropertiesOnConnection(databaseOptions.get());
                    return;
                }

                // we know for sure it is not present, so execute the statement
                statement.executeUpdate(use("DATABASE", databaseName));

                // as of Mar 2025 we know that as a side effect of calling "use database <xxx>" we are updating the database parameter on the connection.
                // so if we want to use the value from cache we need to save this value on the connection when we use the cached connection
                List<Pair<String, String>> cachedValuesForDatabase = List.of(
                        Pair.of("database", fireboltConnection.getSessionProperties().getDatabase()));
                connectionCache.setDatabaseOptions(databaseName, new DatabaseOptions(cachedValuesForDatabase));
                cacheService.put(cacheKey, connectionCache);
            }
        }
    }

    private void updateDatabasePropertiesOnConnection(DatabaseOptions databaseOptions) throws SQLException {
        log.debug("Using cache verification of database");
        // need to set the values on the connection that were cached on the original use database call
        for (Pair<String, String> pair : databaseOptions.getParameters()) {
            fireboltConnection.addProperty(pair.getKey(), pair.getValue(), DO_NOT_VALIDATE_CONNECTION_FLAG);
        }
    }

    /**
     * Once we verify an engine with the backend we will cache its result and would consider the engine valid in between connection establishing.
     * If the engine was deleted between the time a connection cached the engine and the second connection using the cached value, then this would only
     * be caught while executing the statement, not during connection time.
     *
     * @param statement - statement that will execute the verification of the database
     * @param engineName - the name of the engine to check

     */
    private void getAndSetEngineProperties(Statement statement, String engineName, final Optional<ConnectionCache> connectionCacheOptional, final CacheService cacheService, final CacheKey cacheKey) throws SQLException {
        // if the connection cache is empty it means it is not cachable
        if (connectionCacheOptional.isEmpty()) {
            // if no caching of the result then just make the call
            statement.executeUpdate(use("ENGINE", engineName));
        } else {
            // check the cache first
            ConnectionCache connectionCache = connectionCacheOptional.get();
            Optional<EngineOptions> engineOptions = connectionCache.getEngineOptions(engineName);

            if (engineOptions.isPresent()) {
                updateEngineOptionsOnConnection(engineOptions.get());
                return;
            }

            synchronized (connectionCache) {
                // double check another thread did not already update the engine
                engineOptions = connectionCache.getEngineOptions(engineName);

                if (engineOptions.isPresent()) {
                    updateEngineOptionsOnConnection(engineOptions.get());
                    return;
                }

                // we know for sure it is not present, so execute the statement
                statement.executeUpdate(use("ENGINE", engineName));

                // as of Mar 2025 we know that the side effect of calling "use engine <xxx>" we are updating the endpoint and the engine parameter.
                // so if we want to use the value from cache we need to save this value on the connection when we use the cached connection
                List<Pair<String, String>> engineProperties = List.of(Pair.of("engine", fireboltConnection.getSessionProperties().getEngine()));
                connectionCache.setEngineOptions(engineName, new EngineOptions(fireboltConnection.getEndpoint(), engineProperties));
                cacheService.put(cacheKey, connectionCache);
            }
        }

    }

    private void updateEngineOptionsOnConnection(EngineOptions engineOptions) throws SQLException {
        log.debug("Using cache verification of engine");

        // set the engine url
        fireboltConnection.setEndpoint(engineOptions.getEngineUrl());

        // need to set the values on the connection that were cached on the original use database call
        for (Pair<String, String> pair : engineOptions.getParameters()) {
            fireboltConnection.addProperty(pair.getKey(), pair.getValue(), DO_NOT_VALIDATE_CONNECTION_FLAG);
        }
    }

    private String use(String entity, String name) {
        return format("USE %s \"%s\"", entity, name);
    }

}
