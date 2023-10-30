package com.firebolt.jdbc.client.account.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder
public class FireboltDefaultDatabaseEngineResponse {
	@JsonProperty("engine_url")
	String engineUrl;
}
