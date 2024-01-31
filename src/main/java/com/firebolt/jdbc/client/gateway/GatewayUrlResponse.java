package com.firebolt.jdbc.client.gateway;

import org.json.JSONObject;


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
}
