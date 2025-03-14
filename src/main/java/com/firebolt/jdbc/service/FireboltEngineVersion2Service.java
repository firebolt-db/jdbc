package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.tuple.Pair;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class FireboltEngineVersion2Service implements FireboltEngineService {

    // by default cache the values for 1hour
    private static final long DEFAULT_CACHED_VERIFIED_DATABASES_IN_SECONDS = TimeUnit.HOURS.toSeconds(1);
    private static final long DEFAULT_CACHED_VERIFIED_ENGINES_IN_SECONDS = TimeUnit.HOURS.toSeconds(1);

    private static final ExpiringMap<String, List<Pair<String, String>>> CACHED_VERIFIED_DATABASES = ExpiringMap.builder()
            .variableExpiration().build();
    private static final ExpiringMap<String, List<Pair<String, String>>> CACHED_VERIFIED_ENGINES = ExpiringMap.builder()
            .variableExpiration().build();

    private final FireboltConnection fireboltConnection;
    private final long cacheDatabaseDurationInSeconds;
    private final long cacheEngineDurationInSeconds;

    public FireboltEngineVersion2Service(FireboltConnection fireboltConnection) {
        this(fireboltConnection, DEFAULT_CACHED_VERIFIED_DATABASES_IN_SECONDS, DEFAULT_CACHED_VERIFIED_ENGINES_IN_SECONDS);
    }

    // visible for testing
    FireboltEngineVersion2Service(FireboltConnection fireboltConnection, long cachedDatabaseDuration, long cachedEngineDuration) {
        this.fireboltConnection = fireboltConnection;
        this.cacheDatabaseDurationInSeconds = cachedDatabaseDuration;
        this.cacheEngineDurationInSeconds = cachedEngineDuration;
    }

    @Override
    @SuppressWarnings("java:S2077") // Formatting SQL queries is security-sensitive - looks safe in this case
    public Engine getEngine(FireboltProperties properties) throws SQLException {
        try (Statement statement = fireboltConnection.createStatement()) {
            if (properties.getDatabase() != null) {
                verifyDatabaseExists(statement, properties.getHost(), properties.getDatabase());
            }
            verifyEngineExists(statement, properties.getHost(), properties.getEngine());
        }
        // now session properties are updated with new database and engine
        FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
        return new Engine(fireboltConnection.getEndpoint(), null, sessionProperties.getEngine(), sessionProperties.getDatabase(), null);
    }

    /**
     * We are caching the database names for some time and if we had checked it before assumed that it is still valid.
     *
     * @param databaseName
     */
    private void verifyDatabaseExists(Statement statement, String host, String databaseName) throws SQLException {
        synchronized (CACHED_VERIFIED_DATABASES) {

            if (CACHED_VERIFIED_DATABASES.containsKey(asCacheKey(host, databaseName))) {
                log.debug("Using cache verification of database");

                // need to set the values on the connection that were cached on the original use database call
                for (Pair<String, String> pair : CACHED_VERIFIED_DATABASES.get(asCacheKey(host, databaseName))) {
                    fireboltConnection.addProperty(pair.getKey(), pair.getValue());
                }

                return;
            }

            statement.executeUpdate(use("DATABASE", databaseName));

            // as of Mar 2025 we know that as a side effect of calling "use database <xxx>" we are updating the database parameter on the connection.
            // so if we want to use the value from cache we need to save this value on the connection when we use the cached connection
            List<Pair<String, String>> cachedValuesForDatabase = List.of(
                    Pair.of("database", fireboltConnection.getSessionProperties().getDatabase()));
            CACHED_VERIFIED_DATABASES.put(asCacheKey(host, databaseName), cachedValuesForDatabase, cacheDatabaseDurationInSeconds, SECONDS);
        }
    }

    /**
     * We are caching the database names for 15 mins and if we had checked it before assumed that it is still valid.
     *
     * @param engineName
     */
    private void verifyEngineExists(Statement statement, String host, String engineName) throws SQLException {
        synchronized (CACHED_VERIFIED_ENGINES) {
            if (CACHED_VERIFIED_ENGINES.containsKey(asCacheKey(host, engineName))) {
                log.debug("Using cache verification of engine");

                // need to set the values on the connection that were cached on the original use database call
                for (Pair<String, String> pair : CACHED_VERIFIED_ENGINES.get(asCacheKey(host, engineName))) {
                    if ("endpoint".equals(pair.getKey())) {
                        fireboltConnection.setEndpoint(pair.getValue());
                    } else {
                        fireboltConnection.addProperty(pair.getKey(), pair.getValue());
                    }
                }

                return;
            }

            statement.executeUpdate(use("ENGINE", engineName));

            // as of Mar 2025 we know that the side effect of calling "use engine <xxx>" we are updating the endpoint and the engine parameter.
            // so if we want to use the value from cache we need to save this value on the connection when we use the cached connection
            List<Pair<String, String>> cachedValuesForEngine = List.of(
                    Pair.of("engine", fireboltConnection.getSessionProperties().getEngine()),
                    Pair.of("endpoint", fireboltConnection.getEndpoint()));

            CACHED_VERIFIED_ENGINES.put(asCacheKey(host, engineName), cachedValuesForEngine, cacheEngineDurationInSeconds, SECONDS);
        }

    }

    private String use(String entity, String name) {
        return format("USE %s \"%s\"", entity, name);
    }

    private String asCacheKey(String host, String databaseName) {
        return new StringBuilder(host).append(":").append(databaseName).toString();
    }
}
