package com.firebolt.jdbc.client.authentication;

import org.apache.hc.core5.http.HttpEntity;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface AuthenticationRequest {
	HttpEntity getHttpEntity() throws JsonProcessingException;

	String getUri();
}