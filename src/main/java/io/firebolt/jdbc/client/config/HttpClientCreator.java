package io.firebolt.jdbc.client.config;

import io.firebolt.jdbc.client.ssl.InsecureTrustManager;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Class to configure the http client based on the Firebolt properties (connection settings and params)
 */
public class HttpClientCreator {

    private static final String SSL_STRICT_MODE = "strict";
    private static final String SSL_NONE_MODE = "none";
    private static final String TLS_PROTOCOL = "TLS";
    private static final String JKS_KEYSTORE_TYPE = "jks";

    private HttpClientCreator() {
    }

    public static synchronized CloseableHttpClient createClient(FireboltProperties properties) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        return HttpClientBuilder.create()
                .setConnectionManager(getConnectionManager(properties))
                .setRetryHandler(getRequestRetryHandler(properties))
                .setKeepAliveStrategy(createKeepAliveStrategy(properties))
                .setConnectionReuseStrategy(getConnectionReuseStrategy())
                .setDefaultConnectionConfig(getConnectionConfig(properties))
                .setDefaultRequestConfig(getRequestConfig(properties))
                .disableContentCompression()
                .disableRedirectHandling().build();
    }

    private static ConnectionKeepAliveStrategy createKeepAliveStrategy(FireboltProperties fireboltProperties) {
        return (httpResponse, httpContext) -> {
            if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                return -1;
            }
            HeaderElementIterator it = new BasicHeaderElementIterator(httpResponse.headerIterator(HTTP.CONN_DIRECTIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                if (param != null && param.equalsIgnoreCase(HTTP.CONN_KEEP_ALIVE)) {
                    return fireboltProperties.getKeepAliveTimeout();
                }
            }
            return -1;
        };
    }


    private static RequestConfig getRequestConfig(FireboltProperties fireboltProperties) {
        return RequestConfig.custom()
                .setSocketTimeout(fireboltProperties.getSocketTimeout())
                .setConnectTimeout(fireboltProperties.getConnectionTimeout())
                .setConnectionRequestTimeout(fireboltProperties.getConnectionTimeout())
                .setCookieSpec(CookieSpecs.STANDARD)
                .build();
    }

    private static HttpRequestRetryHandler getRequestRetryHandler(FireboltProperties fireboltProperties) {
        final int maxRetries = fireboltProperties.getMaxRetries();
        return new DefaultHttpRequestRetryHandler(maxRetries, false) {
            @Override
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                if (executionCount > maxRetries || context == null || !Boolean.TRUE.equals(context.getAttribute("is_idempotent"))) {
                    return false;
                }

                return (exception instanceof NoHttpResponseException) || super.retryRequest(exception, executionCount, context);
            }
        };
    }

    private static ConnectionConfig getConnectionConfig(FireboltProperties fireboltProperties) {
        return ConnectionConfig.custom()
                .setBufferSize(fireboltProperties.getApacheBufferSize())
                .build();
    }

    private static ConnectionReuseStrategy getConnectionReuseStrategy() {
        return new DefaultConnectionReuseStrategy() {
            @Override
            public boolean keepAlive(HttpResponse httpResponse, HttpContext httpContext) {
                if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    return false;
                }
                return super.keepAlive(httpResponse, httpContext);
            }
        };
    }

    private static PoolingHttpClientConnectionManager getConnectionManager(FireboltProperties fireboltProperties)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        RegistryBuilder<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory());

        if (Boolean.TRUE.equals(fireboltProperties.getSsl())) {
            HostnameVerifier verifier = SSL_STRICT_MODE.equals(fireboltProperties.getSslMode()) ? SSLConnectionSocketFactory.getDefaultHostnameVerifier() : NoopHostnameVerifier.INSTANCE;
            registry.register("https", new SSLConnectionSocketFactory(getSSLContext(fireboltProperties), verifier));
        }

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                registry.build(),
                null,
                null,
                new DnsResolverByIpVersionPriority(),
                fireboltProperties.getTimeToLiveMillis(),
                TimeUnit.MILLISECONDS
        );

        connectionManager.setValidateAfterInactivity(fireboltProperties.getValidateAfterInactivityMillis());
        connectionManager.setDefaultMaxPerRoute(fireboltProperties.getDefaultMaxPerRoute());
        connectionManager.setMaxTotal(fireboltProperties.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(getConnectionConfig(fireboltProperties));
        return connectionManager;
    }


    private static SSLContext getSSLContext(FireboltProperties fireboltProperties)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance(TLS_PROTOCOL);
        TrustManager[] trustManagers = null;
        KeyManager[] keyManagers = null;
        SecureRandom secureRandom = null;

        if (SSL_NONE_MODE.equals(fireboltProperties.getSslMode())) {
            trustManagers = new TrustManager[]{new InsecureTrustManager()};
            keyManagers = new KeyManager[]{};
            secureRandom = new SecureRandom();
        } else if (fireboltProperties.getSslMode().equals(SSL_STRICT_MODE)) {
            if (!fireboltProperties.getSslRootCertificate().isEmpty()) {
                TrustManagerFactory trustManagerFactory = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());

                trustManagerFactory.init(getKeyStore(fireboltProperties));
                trustManagers = trustManagerFactory.getTrustManagers();
                keyManagers = new KeyManager[]{};
                secureRandom = new SecureRandom();
            }
        } else {
            throw new IllegalArgumentException(String.format("The ssl mode %s does not exist", fireboltProperties.getSslMode()));
        }

        ctx.init(keyManagers, trustManagers, secureRandom);
        return ctx;
    }

    private static KeyStore getKeyStore(FireboltProperties fireboltProperties) throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(JKS_KEYSTORE_TYPE);
            keyStore.load(null, null);
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException("jks KeyStore not available");
        }

        InputStream caInputStream;
        try {
            caInputStream = new FileInputStream(fireboltProperties.getSslRootCertificate());
        } catch (FileNotFoundException ex) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            caInputStream = cl.getResourceAsStream(fireboltProperties.getSslRootCertificate());
            if (caInputStream == null) {
                throw new IOException("Could not open SSL/TLS root certificate file '" + fireboltProperties.getSslRootCertificate() + "'", ex);
            }
        }

        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Iterator<? extends Certificate> caIt = cf.generateCertificates(caInputStream).iterator();
            for (int i = 0; caIt.hasNext(); i++) {
                keyStore.setCertificateEntry("cert" + i, caIt.next());
            }
            return keyStore;
        } finally {
            caInputStream.close();
        }
    }

}
