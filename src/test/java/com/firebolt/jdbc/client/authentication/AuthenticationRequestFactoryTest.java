package com.firebolt.jdbc.client.authentication;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AuthenticationRequestFactoryTest {

	@Test
	void shouldGetServiceAccountRequestWhenUsernameDoesNotContainSpecialCharacter() {
		String name = "265576ea-2478-4209-860c-f75f55e7c1f7";
		String password = "hello";
		AuthenticationRequest rq = AuthenticationRequestFactory.getAuthenticationRequest(name, password,
				"localhost");
		assertTrue(rq instanceof ServiceAccountAuthenticationRequest);
	}

	@Test
	void shouldGetUsernamePasswordRqWhenUsernameIsAnEmailAddress() {
		String name = "tester@firebolt.io";
		String password = "hello";
		AuthenticationRequest rq = AuthenticationRequestFactory.getAuthenticationRequest(name, password,
				"localhost");
		assertTrue(rq instanceof UsernamePasswordAuthenticationRequest);
	}

	@Test
	void shouldGetUsernamePasswordRqWhenUsernameIsNullOrEmpty() {
		assertTrue(AuthenticationRequestFactory.getAuthenticationRequest(null, null,
				null) instanceof UsernamePasswordAuthenticationRequest);
		assertTrue(AuthenticationRequestFactory.getAuthenticationRequest("", null,
				null) instanceof UsernamePasswordAuthenticationRequest);
	}
}