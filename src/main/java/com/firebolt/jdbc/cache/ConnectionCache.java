package com.firebolt.jdbc.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;

/**
 * This class encapsulates what information we cache for each connection
 */
public class ConnectionCache {

    @Getter
    private String connectionId;

    @Setter
    @Getter
    private String accessToken;

    @Setter
    @Getter
    private String systemEngineUrl;

    /**
     * On one connection cache we might store information about multiple databases
     */
    private Map<String, DatabaseOptions> databaseOptionsMap;

    /**
     * On one connection cache we might store information about multiple engines
     */
    private Map<String, EngineOptions> engineOptionsMap;

    public ConnectionCache(String connectionId) {
        this.connectionId = connectionId;
        this.databaseOptionsMap = new ConcurrentHashMap<>();
        this.engineOptionsMap = new ConcurrentHashMap<>();
    }

    public Optional<DatabaseOptions> getDatabaseOptions(String databaseName) {
        return Optional.ofNullable(databaseOptionsMap.get(databaseName));
    }

    public void setDatabaseOptions(String databaseName, DatabaseOptions databaseOptions) {
        databaseOptionsMap.put(databaseName, databaseOptions);
    }

    public Optional<EngineOptions> getEngineOptions(String engineName) {
        return Optional.ofNullable(engineOptionsMap.get(engineName));
    }

    public void setEngineOptions(String engineName, EngineOptions engineOptions) {
        engineOptionsMap.put(engineName, engineOptions);
    }

}
