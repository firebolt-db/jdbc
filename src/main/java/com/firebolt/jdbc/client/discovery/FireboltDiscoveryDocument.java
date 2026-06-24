package com.firebolt.jdbc.client.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.json.JSONObject;

@Getter
public class FireboltDiscoveryDocument {

    private static final String AUTH_NONE = "none";
    private static final String AUTH_NO_AUTH = "no_auth";

    private final String authType;
    private final String tokenEndpoint;
    private final String queryEndpoint;
    private final Map<String, String> parameters;

    FireboltDiscoveryDocument(JSONObject json) {
        JSONObject auth = optObject(json, "auth", "authentication");
        JSONObject endpoints = optObject(json, "endpoints", "endpoint");

        authType = firstString(auth, "type", "mode", "auth_type", "authType")
                .or(() -> firstString(json, "auth_type", "authType"))
                .orElse(AUTH_NONE);
        tokenEndpoint = firstString(auth, "token_endpoint", "tokenEndpoint", "oauth_token_endpoint", "oauthTokenEndpoint", "url")
                .or(() -> firstString(json, "token_endpoint", "tokenEndpoint", "oauth_token_endpoint", "oauthTokenEndpoint"))
                .orElse(null);
        queryEndpoint = firstString(endpoints, "query", "query_url", "queryUrl", "engine_url", "engineUrl", "endpoint", "url")
                .or(() -> firstString(json, "query", "query_url", "queryUrl", "engine_url", "engineUrl", "endpoint", "url"))
                .orElse(null);
        parameters = readParameters(json);
    }

    public boolean requiresAuthentication() {
        return tokenEndpoint != null && !AUTH_NONE.equalsIgnoreCase(authType) && !AUTH_NO_AUTH.equalsIgnoreCase(authType);
    }

    private static Map<String, String> readParameters(JSONObject json) {
        Map<String, String> values = new HashMap<>();
        putAll(values, optObject(json, "parameters", "settings", "session"));
        putAll(values, optObject(optObject(json, "endpoints", "endpoint"), "parameters", "settings", "session"));
        return values;
    }

    private static void putAll(Map<String, String> values, JSONObject json) {
        if (json == null) {
            return;
        }
        for (String key : json.keySet()) {
            Object value = json.opt(key);
            if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                values.put(key, String.valueOf(value));
            }
        }
    }

    private static JSONObject optObject(JSONObject json, String... names) {
        if (json == null) {
            return null;
        }
        for (String name : names) {
            JSONObject object = json.optJSONObject(name);
            if (object != null) {
                return object;
            }
        }
        return null;
    }

    private static Optional<String> firstString(JSONObject json, String... names) {
        if (json == null) {
            return Optional.empty();
        }
        for (String name : names) {
            String value = json.optString(name, null);
            if (value != null && !value.isBlank()) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}
