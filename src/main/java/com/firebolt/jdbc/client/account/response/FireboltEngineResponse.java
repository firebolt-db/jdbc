package com.firebolt.jdbc.client.account.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FireboltEngineResponse {
	Engine engine;

	@Value
	@Builder
	public static class Engine {
		String endpoint;
		@JsonProperty("current_status")
		String currentStatus;
	}
}
