package com.firebolt.jdbc.client;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import com.firebolt.jdbc.client.config.HttpClientCreator;
import com.firebolt.jdbc.connection.settings.FireboltProperties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpClientConfig {

	private static CloseableHttpClient client;

	private HttpClientConfig() {
	}

	public static CloseableHttpClient init(FireboltProperties fireboltProperties) throws CertificateException,
			NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
		client = HttpClientCreator.createClient(fireboltProperties);
		log.info("Http client initialized");
		return client;
	}

	public static CloseableHttpClient getInstance() {
		return client;
	}
}
