package com.firebolt.jdbc.client;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import com.firebolt.jdbc.client.config.OkHttpClientCreator;
import com.firebolt.jdbc.connection.settings.FireboltProperties;

import lombok.CustomLog;
import okhttp3.OkHttpClient;

@CustomLog
public class HttpClientConfig {

	private static OkHttpClient instance;

	private HttpClientConfig() {
	}

	public static synchronized OkHttpClient init(FireboltProperties fireboltProperties) throws CertificateException,
			NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
		if (instance == null) {
			instance = OkHttpClientCreator.createClient(fireboltProperties);
			log.info("Http client initialized");
		}
		return instance;
	}

	public static OkHttpClient getInstance() {
		return instance;
	}
}
