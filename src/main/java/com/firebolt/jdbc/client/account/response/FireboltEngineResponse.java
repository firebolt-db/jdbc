package com.firebolt.jdbc.client.account.response;

import org.json.JSONObject;

public class FireboltEngineResponse {
	private final Engine engine;

    public FireboltEngineResponse(Engine engine) {
        this.engine = engine;
    }

	FireboltEngineResponse(JSONObject json) {
		this(new Engine(json.getJSONObject("engine")));
	}

	public Engine getEngine() {
		return engine;
	}

	public static class Engine {
		private final String endpoint;
		private final String currentStatus;

        public Engine(String endpoint, String currentStatus) {
            this.endpoint = endpoint;
            this.currentStatus = currentStatus;
        }

		private Engine(JSONObject json) {
			this(json.getString("endpoint"), json.getString("current_status"));
		}

		public String getEndpoint() {
			return endpoint;
		}

		public String getCurrentStatus() {
			return currentStatus;
		}
	}
}
