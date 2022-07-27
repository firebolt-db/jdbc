package com.firebolt.jdbc.client.ssl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class InsecureTrustManagerTest {

	@Test
	void shouldReturnNoIssuer() {
		InsecureTrustManager insecureTrustManager = new InsecureTrustManager();
		assertEquals(0, insecureTrustManager.getAcceptedIssuers().length);
	}
}
