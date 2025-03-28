package com.firebolt.jdbc.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

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
     * Will have as source either Memory or Disk, if the cache was retrieved from memory or disk
     */
    @Getter
    @Setter
    String cacheSource;

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

    /**
     * Will use this string to generate the checksum
     * @return
     */
    public String asChecksumString() {
        StringBuilder builder = new StringBuilder();

        if (StringUtils.isNotBlank(connectionId)) {
            builder.append("connId:").append(connectionId);
        }

        if (StringUtils.isNotBlank(accessToken)) {
            builder.append("token:").append(accessToken);
        }

        if (StringUtils.isNotBlank(systemEngineUrl)) {
            builder.append("systemEngineUrl:").append(systemEngineUrl);
        }

        if (databaseOptionsMap != null && !databaseOptionsMap.isEmpty()) {
            databaseOptionsMap.entrySet().stream().forEach(entry -> builder.append("database[").append(entry.getKey()).append("]").append(entry.getValue().hashCode()));
        }

        if (engineOptionsMap != null && !engineOptionsMap.isEmpty()) {
            engineOptionsMap.entrySet().stream().forEach(entry -> builder.append("engine[").append(entry.getKey()).append("]").append(entry.getValue().hashCode()));
        }

        return builder.toString();
    }

}
