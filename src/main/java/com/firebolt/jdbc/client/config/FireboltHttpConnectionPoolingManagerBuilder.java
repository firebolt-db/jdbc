package com.firebolt.jdbc.client.config;

import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SchemePortResolver;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.LayeredConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.URIScheme;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.HttpConnectionFactory;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;

import com.firebolt.jdbc.connection.settings.FireboltProperties;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class FireboltHttpConnectionPoolingManagerBuilder {

	HttpConnectionFactory<ManagedHttpClientConnection> connectionFactory;
	LayeredConnectionSocketFactory sslSocketFactory;
	SchemePortResolver schemePortResolver;
	DnsResolver dnsResolver;
	PoolConcurrencyPolicy poolConcurrencyPolicy;
	PoolReusePolicy poolReusePolicy;
	SocketConfig defaultSocketConfig;
	boolean systemProperties;
	int maxConnTotal;
	int maxConnPerRoute;
	TimeValue timeToLive;
	TimeValue validateAfterInactivity;

	FireboltProperties fireboltProperties;

	public FireboltHttpConnectionPoolingManager create() {
		final FireboltHttpConnectionPoolingManager poolingManager = new FireboltHttpConnectionPoolingManager(
				RegistryBuilder.<ConnectionSocketFactory>create()
						.register(URIScheme.HTTP.id, PlainConnectionSocketFactory.getSocketFactory())
						.register(URIScheme.HTTPS.id, getConnectionSocketFactory()).build(),
				poolConcurrencyPolicy, poolReusePolicy, timeToLive != null ? timeToLive : TimeValue.NEG_ONE_MILLISECOND,
				schemePortResolver, dnsResolver, connectionFactory, fireboltProperties);
		if (validateAfterInactivity != null) {
			poolingManager.setValidateAfterInactivity(validateAfterInactivity);
		}
		if (defaultSocketConfig != null) {
			poolingManager.setDefaultSocketConfig(defaultSocketConfig);
		}
		if (maxConnTotal > 0) {
			poolingManager.setMaxTotal(maxConnTotal);
		}
		if (maxConnPerRoute > 0) {
			poolingManager.setDefaultMaxPerRoute(maxConnPerRoute);
		}
		return poolingManager;
	}

	private ConnectionSocketFactory getConnectionSocketFactory() {
		if (sslSocketFactory != null) {
			return sslSocketFactory;
		} else {
			return systemProperties ? SSLConnectionSocketFactory.getSystemSocketFactory()
					: SSLConnectionSocketFactory.getSocketFactory();
		}
	}
}
