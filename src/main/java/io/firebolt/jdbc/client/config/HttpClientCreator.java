package io.firebolt.jdbc.client.config;

import io.firebolt.jdbc.client.ssl.InsecureTrustManager;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.cookie.StandardCookieSpec;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.HeaderElements;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.impl.DefaultConnectionReuseStrategy;
import org.apache.hc.core5.http.io.HttpConnectionFactory;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.message.MessageSupport;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.TRUE;

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
        .setConnectionManager(getConnectionManager(properties))
        .setConnectionManagerShared(true)
        .disableDefaultUserAgent()
        .setDefaultRequestConfig(getDefaultRequestConfig(properties))
        .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
        .setKeepAliveStrategy(createKeepAliveStrategy(properties))
        .setRetryStrategy(createRetryStrategy(properties))
        .disableContentCompression()
        .disableRedirectHandling()
        .build();
  }

  private static HttpRequestRetryStrategy createRetryStrategy(FireboltProperties properties) {
    return new DefaultHttpRequestRetryStrategy(
        properties.getMaxRetries(), TimeValue.ofMilliseconds(500));
  }

  private static ConnectionKeepAliveStrategy createKeepAliveStrategy(
      FireboltProperties fireboltProperties) {
    return (httpResponse, httpContext) -> {
      final Iterator<HeaderElement> it =
          MessageSupport.iterate(httpResponse, HeaderElements.KEEP_ALIVE);
      while (it.hasNext()) {
        final HeaderElement he = it.next();
        final String param = he.getName();
        final String value = he.getValue();
        if (value != null && param.equalsIgnoreCase("timeout")) {
          try {
            return TimeValue.ofSeconds(Long.parseLong(value));
          } catch (final NumberFormatException ignore) {
          }
        }
      }
      return TimeValue.ofMilliseconds(fireboltProperties.getKeepAliveTimeoutMillis());
    };
  }

  private static RequestConfig getDefaultRequestConfig(FireboltProperties fireboltProperties) {
    return RequestConfig.custom()
        .setConnectTimeout(
            Timeout.of(fireboltProperties.getConnectionTimeoutMillis(), TimeUnit.MILLISECONDS))
        .setCookieSpec(StandardCookieSpec.RELAXED)
        .build();
  }

  private static PoolingHttpClientConnectionManager getConnectionManager(
      FireboltProperties fireboltProperties)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException,
          KeyManagementException {
    Http1Config customHttpConfig =
        Http1Config.custom().setBufferSize(fireboltProperties.getBufferSize()).build();

    HttpConnectionFactory<ManagedHttpClientConnection> connectionFactory =
        ManagedHttpClientConnectionFactory.builder().http1Config(customHttpConfig).build();


    HostnameVerifier verifier = getHostnameVerifier(fireboltProperties);
    SSLConnectionSocketFactory sslConnectionSocketFactory =
        TRUE.equals(fireboltProperties.getSsl())
            ? new SSLConnectionSocketFactory(getSSLContext(fireboltProperties), verifier)
            : null;

    return FireboltHttpConnectionPoolingManagerBuilder.builder()
        .sslSocketFactory(sslConnectionSocketFactory)
        .dnsResolver(new DnsResolverByIpVersionPriority())
        .connectionFactory(connectionFactory)
        .maxConnPerRoute(fireboltProperties.getMaxConnectionsPerRoute())
        .validateAfterInactivity(TimeValue.ofMinutes(1))
        .maxConnTotal(fireboltProperties.getMaxConnectionsTotal())
        .timeToLive(TimeValue.ofMilliseconds(fireboltProperties.getTimeToLiveMillis()))
        .defaultSocketConfig(
            SocketConfig.custom()
                .setSoKeepAlive(true)
                    .setSoReuseAddress(true)
                    .setTcpNoDelay(true)
                .setSoLinger(TimeValue.ofMilliseconds(Integer.MAX_VALUE))
                .setSoTimeout(Timeout.ofMilliseconds(fireboltProperties.getSocketTimeoutMillis()))
                .build()).fireboltProperties(fireboltProperties)
        .build().create();
  }

  private static HostnameVerifier getHostnameVerifier(FireboltProperties fireboltProperties) {
    return SSL_STRICT_MODE.equals(fireboltProperties.getSslMode())
        ? new DefaultHostnameVerifier()
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
