package com.firebolt.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class FireboltObjectMapperTest {

	@Test
	void shouldGetInstance() {
		assertInstanceOf(ObjectMapper.class, FireboltObjectMapper.getInstance());
	}
}
