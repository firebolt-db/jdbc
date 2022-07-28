package com.firebolt.jdbc.client.account.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FireboltDatabaseResponse {
	@JsonProperty("engine_url")
	String engineUrl;
}
