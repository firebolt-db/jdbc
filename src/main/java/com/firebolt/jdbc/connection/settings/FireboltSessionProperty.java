package com.firebolt.jdbc.connection.settings;

import lombok.Getter;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.stream.Collectors.toMap;

@Getter
public enum FireboltSessionProperty {
	PATH("path", "", String.class, "Path component of the URI", FireboltProperties::getPath),
	BUFFER_SIZE("buffer_size", 65536, Integer.class, "The buffer used to create the ResultSet in bytes", FireboltProperties::getBufferSize),
	SSL("ssl", true, Boolean.class, "Enable SSL/TLS for the connection", FireboltProperties::isSsl),
	SSL_CERTIFICATE_PATH("ssl_certificate_path", "", String.class, "SSL/TLS root certificate", FireboltProperties::getSslCertificatePath, "sslrootcert"),
	SSL_MODE("ssl_mode", "strict", String.class,
			"SSL mode to verify/not verify the certificate. Supported Types: none (don't verify), strict (verify)", FireboltProperties::getSslMode,
			"sslmode"),
	MAX_RETRIES("max_retries", 3, Integer.class,
			"Maximum number of retries used by the client to query Firebolt when the response has an invalid status code that is retryable (HTTP_CLIENT_TIMEOUT/408, HTTP_BAD_GATEWAY/502, HTTP_UNAVAILABLE/503 or HTTP_GATEWAY_TIMEOUT/504). Set to 0 to disable ", FireboltProperties::getMaxRetries),

	SOCKET_TIMEOUT_MILLIS("socket_timeout_millis", 0, Integer.class,
			"maximum time of inactivity between two data packets when exchanging data with the server. A timeout value of zero is interpreted as an infinite timeout. A negative value is interpreted as undefined.", FireboltProperties::getSocketTimeoutMillis,
			"socket_timeout"),
	CONNECTION_TIMEOUT_MILLIS("connection_timeout_millis", 60 * 1000, Integer.class,
			"Default connect timeout for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE when converted to milliseconds", FireboltProperties::getConnectionTimeoutMillis,
			"connection_timeout"),
	KEEP_ALIVE_TIMEOUT_MILLIS("connection_keep_alive_timeout_millis", 5 * 60 * 1000, Integer.class,
			"How long to keep a connection with the server alive in the pool before closing it.", FireboltProperties::getKeepAliveTimeoutMillis, "keepAliveTimeout"),

	MAX_CONNECTIONS_TOTAL("max_connections_total", 300, Integer.class,
			"Maximum total connections in the connection pool", FireboltProperties::getMaxConnectionsTotal, "maxTotal"),

	TCP_KEEP_IDLE("tcp_keep_idle", 60, Integer.class,
			"TCP option that defines the number of seconds of idle time before keep-alive initiates a probe. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe after some amount of time.", FireboltProperties::getTcpKeepIdle),
	TCP_KEEP_COUNT("tcp_keep_count", 10, Integer.class,
			"TCP option that defines the maximum number of keep-alive probes to be sent. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe a certain number of times before a connection is considered to be broken.", FireboltProperties::getTcpKeepCount),
	TCP_KEEP_INTERVAL("tcp_keep_interval", 30, Integer.class,
			"TCP option that defines the number of seconds to wait before retransmitting a keep-alive probe. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe after some amount of time.", FireboltProperties::getTcpKeepInterval),
	COMPRESS( /*
				 * compress should always be used as the HTTP response code is sometimes
				 * incorrect when not using it
				 */
			"compress", true, Boolean.class, "Whether to compress transferred data or not. Compressed by default", FireboltProperties::isCompress),
	DATABASE("database", null, String.class, "default database name", FireboltProperties::getDatabase),
	// Typically client_secret property should be used, but password is the standard JDBC property supported by all tools, so it is silently defined here as alias. Also see CLIENT_ID.
	CLIENT_SECRET("client_secret", null, String.class, "client secret - null by default", p -> "****", "password"),
	// Typically client_id property should be used, but user is the standard JDBC property supported by all tools, so it is silently defined here as alias. Also see CLIENT_SECRET
	CLIENT_ID("client_id", null, String.class, "client ID - null by default", FireboltProperties::getPrincipal, "user"),
	HOST("host", null, String.class, "Firebolt host - null by default", FireboltProperties::getHost),
	PORT("port", null, Integer.class, "Firebolt port - null by default", FireboltProperties::getPort),
	ENGINE("engine", null, String.class, "engine - null by default", FireboltProperties::getEngine, "engine_name"),
	ACCOUNT("account", null, String.class, "account - null by default", FireboltProperties::getAccount),
	ACCOUNT_ID("account_id", null, String.class, "accountId - null by default", FireboltProperties::getAccountId),
	LOG_RESULT_SET("log_result_set", false, Boolean.class,
			"When set to true, the result of the queries executed are logged with the log level INFO. This has a negative performance impact and should be enabled only for debugging purposes", FireboltProperties::isLogResultSet),
	USER_DRIVERS("user_drivers", null, String.class, "user drivers", FireboltProperties::getUserDrivers),
	USER_CLIENTS("user_clients", null, String.class, "user clients", FireboltProperties::getUserClients),
	ACCESS_TOKEN("access_token", null, String.class, "access token", p -> "***"),
	ENVIRONMENT("environment", "app", String.class, "Firebolt environment", FireboltProperties::getEnvironment, "env"),
	VALIDATE_ON_SYSTEM_ENGINE("validate_on_system_engine", false, Boolean.class,
			"Whether to validate the connection on the system engine or not. By default validates on an engine currently connected.",
			FireboltProperties::isValidateOnSystemEngine),
	// We keep all the deprecated properties to ensure backward compatibility - but
	// they do not have any effect.
	@Deprecated
	TIME_TO_LIVE_MILLIS("time_to_live_millis", 60 * 1000, Integer.class,
			"Maximum life span of connections regardless of their connection_keep_alive_timeout_millis", p -> null,
			"timeToLiveMillis"),
	@Deprecated
	MAX_CONNECTIONS_PER_ROUTE("max_connections_per_route", 500, Integer.class, "Maximum total connections per route", p -> null,
			"defaultMaxPerRoute"),

	@Deprecated
	USE_PATH_AS_DB("use_path_as_db", null, Boolean.class,
			"When set to true (the default) or not specified, the path parameter from the URL is used as the database name", p -> null),

	@Deprecated
	USE_CONNECTION_POOL("use_connection_pool", true, Boolean.class,
			"use connection pool for valid connections. This property is deprecated and setting it has no effect.", p -> null),

	@Deprecated
	VALIDATE_AFTER_INACTIVITY_MILLIS("validate_after_inactivity_millis", 3 * 1000, Integer.class,
			"Defines period of inactivity in milliseconds after which persistent connections must be re-validated prior to being leased to the consumer. Non-positive value disables connection validation. ", p -> null),

	@Deprecated
	CLIENT_BUFFER_SIZE("client_buffer_size", 65536, Integer.class,
			"The buffer for the Apache client used by the Driver (in bytes). It is the preferred buffer size for the body of the http response. A larger buffer allows more content to be written before anything is actually sent while a smaller buffer decreases server memory load and allows the client to start receiving data quicker.\n"
					+ "The buffer will be at least as large as the size requested.", p -> null,
			"apache_buffer_size"),
	@Deprecated
	AGGRESSIVE_CANCEL("aggressive_cancel", false, Boolean.class,
			"enable aggressive cancel. Permits to cancel queries by sending a query to Firebolt rather than calling the /cancel endpoint", p -> null);

	private final String key;
	private final Object defaultValue;
	private final Class<?> clazz;
	private final String description;
	private final Function<FireboltProperties, Object> valueGetter;
	private final String[] aliases;
	private static final Map<String, FireboltSessionProperty> aliasToProperty =
			Arrays.stream(values()).flatMap(FireboltSessionProperty::getAllPropertyMapping).collect(caseInsensitiveMap());

	FireboltSessionProperty(String key, Object defaultValue, Class<?> clazz, String description, Function<FireboltProperties, Object> valueGetter, String... aliases) {
		this.key = key;
		this.defaultValue = defaultValue;
		this.clazz = clazz;
		this.description = description;
		this.valueGetter = valueGetter;
		this.aliases = aliases != null ? aliases : new String[] {};
	}

	public static List<FireboltSessionProperty> getNonDeprecatedProperties() {
		return Arrays.stream(FireboltSessionProperty.values()).filter(value -> {
			try {
				Field field = FireboltSessionProperty.class.getField(value.name());
				return !field.isAnnotationPresent(Deprecated.class);
			} catch (NoSuchFieldException | SecurityException e) {
				return false;
			}
		}).collect(Collectors.toList());
	}

	public static FireboltSessionProperty byAlias(String keyOrAlias) {
		return aliasToProperty.get(keyOrAlias);
	}

	public String[] getPossibleValues() {
		return Boolean.class.equals(clazz) || Boolean.TYPE.equals(clazz) ? new String[] { "true", "false" } : null;
	}

	public Object getValue(FireboltProperties fireboltProperties) {
		return valueGetter.apply(fireboltProperties);
	}

	private static Stream<String> getAllAliases(FireboltSessionProperty property) {
		return Stream.concat(Stream.of(property.key), Arrays.stream(property.aliases));
	}

	private static Stream<Map.Entry<String, FireboltSessionProperty>> getAllPropertyMapping(FireboltSessionProperty property) {
		return getAllAliases(property).map(a -> Map.entry(a, property));
	}

	private static Collector<Entry<String, FireboltSessionProperty>, ?, Map<String, FireboltSessionProperty>> caseInsensitiveMap() {
		return toMap(Entry::getKey, Entry::getValue, (o, t) -> t, () -> new TreeMap<>(CASE_INSENSITIVE_ORDER));
	}
}
