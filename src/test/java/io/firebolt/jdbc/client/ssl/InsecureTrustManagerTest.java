package io.firebolt.jdbc.client.ssl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InsecureTrustManagerTest {

    @Test
    void shouldReturnNoIssuer() {
        InsecureTrustManager insecureTrustManager = new InsecureTrustManager();
        assertEquals(0, insecureTrustManager.getAcceptedIssuers().length);
    }
}