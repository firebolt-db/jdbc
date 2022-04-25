package io.firebolt.jdbc.client.config;

import io.firebolt.jdbc.client.ssl.InsecureTrustManager;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HeaderIterator;
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

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.concurrent.TimeUnit;

/** Class to configure the http client using the session settings */
@UtilityClass
public class HttpClientCreator {

  private static final String SSL_STRICT_MODE = "strict";
  private static final String SSL_NONE_MODE = "none";
  private static final String TLS_PROTOCOL = "TLS";
  private static final String JKS_KEYSTORE_TYPE = "JKS";
  private static final String CERTIFICATE_TYPE_X_509 = "X.509";

  public static CloseableHttpClient createClient(FireboltProperties properties)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException,
          KeyManagementException {
    return HttpClientBuilder.create()
        .setDefaultConnectionConfig(getDefaultConnectionConfig(properties))
        .setDefaultRequestConfig(getDefaultRequestConfig(properties))
        .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
        .setConnectionManager(getConnectionManager(properties))
        .setKeepAliveStrategy(createKeepAliveStrategy(properties))
        .setRetryHandler(new DefaultHttpRequestRetryHandler(properties.getMaxRetries(), false))
        .disableContentCompression()
        .disableRedirectHandling()
        .build();
  }

  private static ConnectionConfig getDefaultConnectionConfig(FireboltProperties properties) {
    return ConnectionConfig.custom().setBufferSize(properties.getClientBufferSize()).build();
  }

  private static ConnectionKeepAliveStrategy createKeepAliveStrategy(
      FireboltProperties fireboltProperties) {
    return (httpResponse, httpContext) -> {
      HeaderIterator headerIterator = httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE);
      HeaderElementIterator elementIterator = new BasicHeaderElementIterator(headerIterator);

      while (elementIterator.hasNext()) {
        HeaderElement element = elementIterator.nextElement();
        String param = element.getName();
        String value = element.getValue();
        if (value != null && param.equalsIgnoreCase("timeout")) {
          return Long.parseLong(value) * 1000;
        }
      }

      return fireboltProperties.getKeepAliveTimeoutMillis();
    };
  }

  private static RequestConfig getDefaultRequestConfig(FireboltProperties fireboltProperties) {
    return RequestConfig.custom()
        .setConnectTimeout(fireboltProperties.getConnectionTimeoutMillis())
        .setConnectionRequestTimeout(fireboltProperties.getConnectionTimeoutMillis())
        .setCookieSpec(CookieSpecs.STANDARD)
        .setSocketTimeout(fireboltProperties.getSocketTimeoutMillis())
        .build();
  }

  private static ConnectionConfig getConnectionConfig(FireboltProperties fireboltProperties) {
    return getDefaultConnectionConfig(fireboltProperties);
  }

  private static PoolingHttpClientConnectionManager getConnectionManager(
      FireboltProperties fireboltProperties)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException,
          KeyManagementException, IOException {

    RegistryBuilder<ConnectionSocketFactory> registry =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.getSocketFactory());

    if (Boolean.TRUE.equals(fireboltProperties.getSsl())) {
      HostnameVerifier verifier = getHostnameVerifier(fireboltProperties);
      registry.register(
          "https", new SSLConnectionSocketFactory(getSSLContext(fireboltProperties), verifier));
    }

    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(
            registry.build(),
            null,
            null,
            new DnsResolverByIpVersionPriority(),
            fireboltProperties.getTimeToLiveMillis(),
            TimeUnit.MILLISECONDS);

    connectionManager.setValidateAfterInactivity(
        fireboltProperties.getValidateAfterInactivityMillis());
    connectionManager.setDefaultMaxPerRoute(fireboltProperties.getMaxConnectionsPerRoute());
    connectionManager.setMaxTotal(fireboltProperties.getMaxConnectionsTotal());
    connectionManager.setDefaultConnectionConfig(getConnectionConfig(fireboltProperties));
    return connectionManager;
  }

  private static HostnameVerifier getHostnameVerifier(FireboltProperties fireboltProperties) {
    return SSL_STRICT_MODE.equals(fireboltProperties.getSslMode())
        ? SSLConnectionSocketFactory.getDefaultHostnameVerifier()
        : NoopHostnameVerifier.INSTANCE;
  }

  private static SSLContext getSSLContext(FireboltProperties fireboltProperties)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException,
          KeyManagementException {
    SSLContext ctx = SSLContext.getInstance(TLS_PROTOCOL);
    TrustManager[] trustManagers;
    KeyManager[] keyManagers;
    SecureRandom secureRandom;

    if (SSL_NONE_MODE.equals(fireboltProperties.getSslMode())) {
      trustManagers = new TrustManager[] {new InsecureTrustManager()};
      keyManagers = new KeyManager[] {};
      secureRandom = new SecureRandom();
    } else if (fireboltProperties.getSslMode().equals(SSL_STRICT_MODE)) {
      if (StringUtils.isNotEmpty(fireboltProperties.getSslCertificatePath())) {
        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(getKeyStore(fireboltProperties));
        trustManagers = trustManagerFactory.getTrustManagers();
        keyManagers = new KeyManager[] {};
        secureRandom = new SecureRandom();
      } else {
        trustManagers = null;
        keyManagers = null;
        secureRandom = null;
      }
    } else {
      throw new IllegalArgumentException(
          String.format("The ssl mode %s does not exist", fireboltProperties.getSslMode()));
    }

    ctx.init(keyManagers, trustManagers, secureRandom);
    return ctx;
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
        throw new IOException(
            String.format(
                "Could not open SSL/TLS certificate file %s",
                fireboltProperties.getSslCertificatePath()),
            ex);
      }
    }
    return caInputStream;
  }
}
