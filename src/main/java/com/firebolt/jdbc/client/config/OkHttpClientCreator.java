package com.firebolt.jdbc.client.config;

import com.firebolt.jdbc.client.config.socket.FireboltSSLSocketFactory;
import com.firebolt.jdbc.client.config.socket.FireboltSocketFactory;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.UtilityClass;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to configure the http client using the session settings
 */
@UtilityClass
public class OkHttpClientCreator {

	private static final String SSL_STRICT_MODE = "strict";
	private static final String SSL_NONE_MODE = "none";
	private static final String TLS_PROTOCOL = "TLS";
	private static final String JKS_KEYSTORE_TYPE = "JKS";
	private static final String CERTIFICATE_TYPE_X_509 = "X.509";
	TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
		@Override
		public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
			// Not checking
		}

		@Override
		public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
			// Not checking
		}

		@Override
		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return new java.security.cert.X509Certificate[] {};
		}
	} };

	public static OkHttpClient createClient(FireboltProperties properties) throws CertificateException,
			NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
		OkHttpClient.Builder builder = new OkHttpClient.Builder()
				.connectTimeout(properties.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS)
				.addInterceptor(new RetryInterceptor(properties.getMaxRetries()))
				.socketFactory(new FireboltSocketFactory(properties))
				.readTimeout(properties.getSocketTimeoutMillis(), TimeUnit.MILLISECONDS)
				.connectionPool(new ConnectionPool(properties.getMaxConnectionsTotal(),
						properties.getKeepAliveTimeoutMillis(), TimeUnit.MILLISECONDS));

		Optional<SSLConfig> sslConfig = getSSLConfig(properties);
		if (sslConfig.isPresent()) {
			SSLContext ctx = SSLContext.getInstance(TLS_PROTOCOL);
			SSLConfig config = sslConfig.get();
			ctx.init(config.getKeyManagers(), config.getTrustManagers(), config.secureRandom);
			builder.sslSocketFactory(new FireboltSSLSocketFactory(properties, ctx.getSocketFactory()),
					(X509TrustManager) config.trustManagers[0]);
		}
		getHostnameVerifier(properties).ifPresent(builder::hostnameVerifier);

		return builder.build();

	}

	private static Optional<HostnameVerifier> getHostnameVerifier(FireboltProperties properties) {
		if (properties.isSsl() && SSL_NONE_MODE.equals(properties.getSslMode())) {
			// No verification when SSL mode is NONE
			return Optional.of((hostname, session) -> true);
		} else {
			return Optional.empty();
		}
	}

	private static Optional<SSLConfig> getSSLConfig(FireboltProperties fireboltProperties) throws CertificateException,
			NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
		if (!fireboltProperties.isSsl()) {
			return Optional.empty();
		}
		TrustManager[] trustManagers;
		KeyManager[] keyManagers;
		SecureRandom secureRandom;
		if (SSL_NONE_MODE.equals(fireboltProperties.getSslMode())) {
			trustManagers = trustAllCerts;
			keyManagers = new KeyManager[] {};
			secureRandom = new SecureRandom();
		} else if (SSL_STRICT_MODE.equals(fireboltProperties.getSslMode())) {
			TrustManagerFactory trustManagerFactory = TrustManagerFactory
					.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			// When null, it uses the default trusted KeyStore
			trustManagerFactory.init(getKeyStore(fireboltProperties).orElse(null));
			trustManagers = trustManagerFactory.getTrustManagers();
			keyManagers = new KeyManager[] {};
			secureRandom = new SecureRandom();
		} else {
			throw new IllegalArgumentException(
					String.format("The ssl mode %s does not exist", fireboltProperties.getSslMode()));
		}
		if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
			throw new IllegalStateException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
		}
		return Optional.of(SSLConfig.builder().keyManagers(keyManagers).trustManagers(trustManagers)
				.secureRandom(secureRandom).build());

	}

	private static Optional<KeyStore> getKeyStore(FireboltProperties fireboltProperties)
			throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
		String sslCertificatePath = fireboltProperties.getSslCertificatePath();
		if (sslCertificatePath == null || sslCertificatePath.isEmpty()) {
			return Optional.empty();
		}
		KeyStore keyStore = KeyStore.getInstance(JKS_KEYSTORE_TYPE);
		try (InputStream certificate = openSslFile(sslCertificatePath)) {
			keyStore.load(null, null);
			CertificateFactory cf = CertificateFactory.getInstance(CERTIFICATE_TYPE_X_509);
			int i = 0;
			for (Certificate value : cf.generateCertificates(certificate)) {
				keyStore.setCertificateEntry(String.format("Certificate_ %d)", i++), value);
			}
			return Optional.of(keyStore);
		}
	}

	private static InputStream openSslFile(String sslCertificatePath) throws IOException {
		InputStream caInputStream;
		try {
			caInputStream = new FileInputStream(sslCertificatePath);
		} catch (FileNotFoundException ex) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			caInputStream = cl.getResourceAsStream(sslCertificatePath);
			if (caInputStream == null) {
				throw new IOException(String.format("Could not open SSL/TLS certificate file %s", sslCertificatePath), ex);
			}
		}
		return caInputStream;
	}

	@Builder
	@Value
	private static class SSLConfig {
		TrustManager[] trustManagers;
		KeyManager[] keyManagers;
		SecureRandom secureRandom;
	}

}
