package com.firebolt.jdbc.client.gateway;

import org.json.JSONObject;

import java.util.Objects;


public class GatewayUrlResponse {
    private final String engineUrl;

    public GatewayUrlResponse(String engineUrl) {
        this.engineUrl = engineUrl;
    }

    GatewayUrlResponse(JSONObject json) {
        this(json.getString("engineUrl"));
    }

    public String getEngineUrl() {
        return engineUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GatewayUrlResponse that = (GatewayUrlResponse) o;
        return Objects.equals(engineUrl, that.engineUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(engineUrl);
    }
}
