package com.firebolt.jdbc.connection;

import org.json.JSONObject;

public class FireboltConnectionTokens {
	private final String accessToken;
	private final long expiresInSeconds;

    public FireboltConnectionTokens(String accessToken, long expiresInSeconds) {
        this.accessToken = accessToken;
        this.expiresInSeconds = expiresInSeconds;
    }

	FireboltConnectionTokens(JSONObject json) {
		this(json.getString("access_token"), json.getLong("expires_in"));
	}

	public String getAccessToken() {
		return accessToken;
	}

	public long getExpiresInSeconds() {
		return expiresInSeconds;
	}
}
