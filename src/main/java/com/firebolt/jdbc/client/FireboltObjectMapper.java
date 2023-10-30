package com.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FireboltObjectMapper {
	private static final ObjectMapper MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private FireboltObjectMapper() {
	}

	public static ObjectMapper getInstance() {
		return MAPPER;
	}
}
