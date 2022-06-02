package io.firebolt.jdbc.client.config;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.Builder;
import lombok.Getter;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SchemePortResolver;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.io.HttpConnectionFactory;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;

class FireboltHttpConnectionPoolingManager extends PoolingHttpClientConnectionManager {

  protected FireboltHttpConnectionPoolingManager(
      final Registry<ConnectionSocketFactory> socketFactoryRegistry,
      final PoolConcurrencyPolicy poolConcurrencyPolicy,
      final PoolReusePolicy poolReusePolicy,
      final TimeValue timeToLive,
      final SchemePortResolver schemePortResolver,
      final DnsResolver dnsResolver,
      final HttpConnectionFactory<ManagedHttpClientConnection> connFactory,
      final FireboltProperties fireboltProperties) {
    super(
        new FireboltHttpConnectionOperator(
            socketFactoryRegistry, schemePortResolver, dnsResolver, fireboltProperties),
        poolConcurrencyPolicy,
        poolReusePolicy,
        timeToLive,
        connFactory);
  }
}
