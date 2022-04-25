package io.firebolt.jdbc.client;


import io.firebolt.jdbc.client.config.HttpClientCreator;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

@Slf4j
public class HttpClientConfig {

    private static CloseableHttpClient client;

    private HttpClientConfig() {
    }

    public static CloseableHttpClient init(FireboltProperties fireboltProperties) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        client = HttpClientCreator.createClient(fireboltProperties);
        log.debug("Http client initialized");
        return client;
    }

    public static CloseableHttpClient getInstance() {
        return client;
    }
}
