package com.firebolt.jdbc.client.account.response;

import org.json.JSONObject;

public class FireboltEngineIdResponse {
	private final Engine engine;

    public FireboltEngineIdResponse(Engine engine) {
        this.engine = engine;
    }

	FireboltEngineIdResponse(JSONObject json) {
		this(new Engine(json.getJSONObject("engine_id")));
	}

	public Engine getEngine() {
		return engine;
	}

	public static class Engine {
		private final String engineId;

        public Engine(String engineId) {
            this.engineId = engineId;
        }

		private Engine(JSONObject json) {
			this(json.getString("engine_id"));
		}

		public String getEngineId() {
			return engineId;
		}
	}
}
