package com.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class FireboltObjectMapper {
	private static final ObjectMapper MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static com.fasterxml.jackson.databind.ObjectMapper getInstance() {
		return MAPPER;
	}
}
