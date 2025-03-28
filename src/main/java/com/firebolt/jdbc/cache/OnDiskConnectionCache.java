package com.firebolt.jdbc.cache;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

/**
 * This would be the json file that we will write to disk
 */
@Getter
public class OnDiskConnectionCache {

    private String connectionId;
    private String encryptedJwtToken;
    private String systemEngineUrl;

    private Map<String, DatabaseOptions> databaseOptionsMap;
    private Map<String, EngineOptions> engineOptionsMap;

    /**
     * The checksum will be applied on: encryptedJwtToken, systemEngineUrl, connectionId, databaseOptionsMap and engineOptionsMap
     */
    String checksum;

    public OnDiskConnectionCache(JSONObject jsonObject) {
        this(jsonObject.getString("connectionId"), jsonObject.getString("encryptedJwtToken"), jsonObject.getString("systemEngineUrl"), jsonObject.getString("checksum"),
                jsonObject.getJSONObject("databaseOptionsMap"), jsonObject.getJSONObject("engineOptionsMap"));
    }

    public OnDiskConnectionCache(String connectionId, String encryptedJwtToken, String systemEngineUrl, Map<String, DatabaseOptions> databaseOptionsMap, Map<String, EngineOptions> engineOptionsMap, String checksum) {
        this.connectionId = connectionId;
        this.encryptedJwtToken = encryptedJwtToken;
        this.systemEngineUrl = systemEngineUrl;
        this.databaseOptionsMap = databaseOptionsMap;
        this.engineOptionsMap = engineOptionsMap;
        this.checksum = checksum;
    }

    private OnDiskConnectionCache(String connectionId, String encryptedJwtToken, String systemEngineUrl, String checksum, JSONObject databaseOptions, JSONObject engineOptions) {
        this.connectionId = connectionId;
        this.encryptedJwtToken = encryptedJwtToken;
        this.systemEngineUrl = systemEngineUrl;
        this.checksum = checksum;

        if (databaseOptions != null) {
            System.out.println(databaseOptions);
            Map<String, Object> deserializedMap = databaseOptions.toMap();

            databaseOptionsMap = deserializedMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, this::asDatabaseOptions));

        }

        if (engineOptions != null) {
            System.out.println(engineOptions);
            Map<String, Object> deserializedMap = engineOptions.toMap();

            engineOptionsMap = deserializedMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, this::asEngineOptions));

        }
    }

    private DatabaseOptions asDatabaseOptions(Map.Entry<String, Object> entry) {
        Map map = (Map) entry.getValue();
        List<Pair<String, String>> parameters = ((List<Map>) map.get("parameters"))
                .stream()
                .map(params -> Pair.of((String) params.get("key"), (String) params.get("value")))
                .collect(Collectors.toList());
        return new DatabaseOptions(parameters);
    }

    private EngineOptions asEngineOptions(Map.Entry<String, Object> entry) {
        Map map = (Map) entry.getValue();
        List<Pair<String, String>> parameters = ((List<Map>) map.get("parameters"))
                .stream()
                .map(params -> Pair.of((String) params.get("key"), (String) params.get("value")))
                .collect(Collectors.toList());
        return new EngineOptions((String) map.get("engineUrl"), parameters);
    }

    public static void main(String[] args) {
        Map<String, DatabaseOptions> databaseOptionsMap = new ConcurrentHashMap<>();
        databaseOptionsMap.put("db1", new DatabaseOptions(List.of(Pair.of("database", "db1"))));
        databaseOptionsMap.put("db2", new DatabaseOptions(List.of(Pair.of("database", "db2"))));
        JSONObject databaseOptions = new JSONObject(databaseOptionsMap);

        Map<String, EngineOptions> engineOptionsMap = new ConcurrentHashMap<>();
        engineOptionsMap.put("engine1", new EngineOptions("engineUrl1", List.of(Pair.of("engine", "engine1"))));
        engineOptionsMap.put("engine2", new EngineOptions("engineUrl2", List.of(Pair.of("engine", "engine2"))));
        JSONObject engineOptions = new JSONObject(engineOptionsMap);

        OnDiskConnectionCache onDiskConnectionCache = new OnDiskConnectionCache("id", "token", "engine", "checksum", databaseOptions, engineOptions);

        JSONObject jsonObject = new JSONObject(onDiskConnectionCache);
        System.out.println(jsonObject);

        OnDiskConnectionCache deserialized = new OnDiskConnectionCache(jsonObject);
        System.out.println(deserialized.getConnectionId());
        System.out.println(deserialized.getEncryptedJwtToken());
        System.out.println(deserialized.getSystemEngineUrl());
        System.out.println(deserialized.getChecksum());
        System.out.println("Database options");
        Map<String, DatabaseOptions> deserializedMap = deserialized.getDatabaseOptionsMap();
        deserializedMap.entrySet().stream().forEach(entry -> {
            System.out.println(entry.getKey());
            entry.getValue().getParameters().stream().forEach(pair -> System.out.println(pair.getKey() + " " + pair.getValue()));
        });
        System.out.println("Engine options");
        Map<String, EngineOptions> engineMap = deserialized.getEngineOptionsMap();
        engineMap.entrySet().stream().forEach(entry -> {
            System.out.println(entry.getKey() + " engine: "+ entry.getValue().getEngineUrl());
            entry.getValue().getParameters().stream().forEach(pair -> System.out.println(pair.getKey() + " " + pair.getValue()));
        });

    }
}
