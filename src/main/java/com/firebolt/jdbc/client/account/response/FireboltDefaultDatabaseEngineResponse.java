package com.firebolt.jdbc.client.account.response;

import org.json.JSONObject;

public class FireboltDefaultDatabaseEngineResponse {
	private final String engineUrl;

    public FireboltDefaultDatabaseEngineResponse(String engineUrl) {
        this.engineUrl = engineUrl;
    }

	FireboltDefaultDatabaseEngineResponse(JSONObject json) {
		this(json.getString("engine_url"));
	}

	public String getEngineUrl() {
		return engineUrl;
	}
}
