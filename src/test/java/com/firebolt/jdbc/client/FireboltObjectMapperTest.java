package com.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class FireboltObjectMapperTest {

  @Test
  void shouldGetInstance() {
    assertInstanceOf(ObjectMapper.class, FireboltObjectMapper.getInstance());
  }
}
