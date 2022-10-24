package com.firebolt.jdbc.client.config;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.UtilityClass;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
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

    public static OkHttpClient createClient(FireboltProperties properties) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        OkHttpClient.Builder builder = new OkHttpClient.Builder().retryOnConnectionFailure(true)
                .connectTimeout(properties.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS)
                .dns(new DnsResolverByIpVersionPriority())
                .readTimeout(properties.getSocketTimeoutMillis(), TimeUnit.MILLISECONDS)
                .connectionPool(new ConnectionPool(properties.getMaxConnectionsTotal(), properties.getTimeToLiveMillis(), TimeUnit.MILLISECONDS));

        getSSLConfig(properties).ifPresent(config -> builder.sslSocketFactory(config.getSslSocketFactory(), config.getTrustManager()));
        getHostnameVerifier(properties).ifPresent(builder::hostnameVerifier);

        return builder.build();

    }

    private static Optional<HostnameVerifier> getHostnameVerifier(FireboltProperties properties) {
        if (properties.isSsl() && SSL_NONE_MODE.equals(properties.getSslMode())) {
            return Optional.of((hostname, session) -> true);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<SSLConfig> getSSLConfig(FireboltProperties fireboltProperties) throws CertificateException,
            NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance(TLS_PROTOCOL);
        if (!fireboltProperties.isSsl()) {
            return Optional.empty();
        }
        TrustManager[] trustManagers;
        KeyManager[] keyManagers;
        SecureRandom secureRandom;
        if (SSL_NONE_MODE.equals(fireboltProperties.getSslMode())) {
            trustManagers = trustAllCerts;
            keyManagers = new KeyManager[]{};
            secureRandom = new SecureRandom();
        } else if (SSL_STRICT_MODE.equals(fireboltProperties.getSslMode())) {
            if (StringUtils.isNotEmpty(fireboltProperties.getSslCertificatePath())) {
                TrustManagerFactory trustManagerFactory = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(getKeyStore(fireboltProperties));
                trustManagers = trustManagerFactory.getTrustManagers();
                keyManagers = new KeyManager[]{};
                secureRandom = new SecureRandom();
            } else {
                //If there is no certificate file then the default config will be used.
                return Optional.empty();
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("The ssl mode %s does not exist", fireboltProperties.getSslMode()));
        }
        if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
            throw new IllegalStateException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
        }
        ctx.init(keyManagers, trustManagers, secureRandom);
        return Optional.of(SSLConfig.builder().sslSocketFactory(ctx.getSocketFactory())
                .trustManager((X509TrustManager) trustManagers[0]).build());
    }

    private static KeyStore getKeyStore(FireboltProperties fireboltProperties)
            throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore keyStore;
        keyStore = KeyStore.getInstance(JKS_KEYSTORE_TYPE);
        try (InputStream certificate = openSslFile(fireboltProperties)) {
            keyStore.load(null, null);
            CertificateFactory cf = CertificateFactory.getInstance(CERTIFICATE_TYPE_X_509);
            int i = 0;
            for (Certificate value : cf.generateCertificates(certificate)) {
                keyStore.setCertificateEntry(String.format("Certificate_ %d)", i++), value);
            }
            return keyStore;
        }
    }

    private static InputStream openSslFile(FireboltProperties fireboltProperties) throws IOException {
        InputStream caInputStream;
        try {
            caInputStream = new FileInputStream(fireboltProperties.getSslCertificatePath());
        } catch (FileNotFoundException ex) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            caInputStream = cl.getResourceAsStream(fireboltProperties.getSslCertificatePath());
            if (caInputStream == null) {
                throw new IOException(String.format("Could not open SSL/TLS certificate file %s",
                        fireboltProperties.getSslCertificatePath()), ex);
            }
        }
        return caInputStream;
    }

    TrustManager[] trustAllCerts = new TrustManager[]{
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                }

                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[]{};
                }
            }
    };


    @Builder
    @Value
    private static class SSLConfig {
        X509TrustManager trustManager;
        SSLSocketFactory sslSocketFactory;
        HostnameVerifier hostnameVerifier;
    }

}
