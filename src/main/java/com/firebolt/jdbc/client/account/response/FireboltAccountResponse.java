package com.firebolt.jdbc.client.account.response;

import org.json.JSONObject;

public class FireboltAccountResponse {
	private final String accountId;

	public FireboltAccountResponse(String accountId) {
		this.accountId = accountId;
	}

	FireboltAccountResponse(JSONObject json) {
		this(json.getString("account_id"));
	}

	public String getAccountId() {
		return accountId;
	}
}
