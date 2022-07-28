package com.firebolt.jdbc.connection.settings;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder(toBuilder = true)
@Slf4j
public class FireboltProperties {

	private static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
	private static final int FIREBOLT_SSL_PROXY_PORT = 443;
	private static final int FIREBOLT_NO_SSL_PROXY_PORT = 9090;

	private static final Set<String> sessionPropertyKeys = Arrays.stream(FireboltSessionProperty.values())
			.map(property -> {
				List<String> keys = new ArrayList<>();
				keys.add(property.getKey());
				keys.addAll(Arrays.asList(property.getAliases()));
				return keys;
			}).flatMap(List::stream).collect(Collectors.toSet());

	int timeToLiveMillis;
	int validateAfterInactivityMillis;
	int maxConnectionsPerRoute;
	int maxConnectionsTotal;
	int maxRetries;
	int bufferSize;
	int clientBufferSize;
	int socketTimeoutMillis;
	int connectionTimeoutMillis;
	int keepAliveTimeoutMillis;
	Integer port;
	String host;
	String database;
	String path;
	boolean ssl;
	String sslCertificatePath;
	String sslMode;
	boolean compress;
	String user;
	String password;
	String engine;
	String account;
	Integer tcpKeepIdle;
	boolean aggressiveCancel;
	Integer tcpKeepCount;
	Integer tcpKeepInterval;
	boolean logResultSet;
	@Builder.Default
	Map<String, String> additionalProperties = new HashMap<>();

	public static FireboltProperties of(Properties... properties) {
		Properties mergedProperties = mergeProperties(properties);
		boolean ssl = getSetting(mergedProperties, FireboltSessionProperty.SSL);
		String sslRootCertificate = getSetting(mergedProperties, FireboltSessionProperty.SSL_CERTIFICATE_PATH);
		String sslMode = getSetting(mergedProperties, FireboltSessionProperty.SSL_MODE);
		boolean compress = getSetting(mergedProperties, FireboltSessionProperty.COMPRESS);
		String user = getSetting(mergedProperties, FireboltSessionProperty.USER);
		String password = getSetting(mergedProperties, FireboltSessionProperty.PASSWORD);
		String path = getSetting(mergedProperties, FireboltSessionProperty.PATH);
		String engine = getSetting(mergedProperties, FireboltSessionProperty.ENGINE);
		String account = getSetting(mergedProperties, FireboltSessionProperty.ACCOUNT);
		int maxConnectionsPerRoute = getSetting(mergedProperties, FireboltSessionProperty.MAX_CONNECTIONS_PER_ROUTE);
		int timeToLiveMillis = getSetting(mergedProperties, FireboltSessionProperty.TIME_TO_LIVE_MILLIS);
		int validateAfterInactivityMillis = getSetting(mergedProperties,
				FireboltSessionProperty.VALIDATE_AFTER_INACTIVITY_MILLIS);
		int maxTotal = getSetting(mergedProperties, FireboltSessionProperty.MAX_CONNECTIONS_TOTAL);
		int maxRetries = getSetting(mergedProperties, FireboltSessionProperty.MAX_RETRIES);
		int bufferSize = getSetting(mergedProperties, FireboltSessionProperty.BUFFER_SIZE);
		int clientBufferSize = getSetting(mergedProperties, FireboltSessionProperty.CLIENT_BUFFER_SIZE);
		int socketTimeout = getSetting(mergedProperties, FireboltSessionProperty.SOCKET_TIMEOUT_MILLIS);
		int connectionTimeout = getSetting(mergedProperties, FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS);
		int keepAliveTimeout = getSetting(mergedProperties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT_MILLIS);

		int tcpKeepInterval = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_INTERVAL);
		int tcpKeepIdle = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_IDLE);
		int tcpKeepCount = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_COUNT);
		boolean aggressiveCancel = getSetting(mergedProperties, FireboltSessionProperty.AGGRESSIVE_CANCEL);

		boolean logResultSet = getSetting(mergedProperties, FireboltSessionProperty.LOG_RESULT_SET);

		String host = getHost(mergedProperties);
		Integer port = getPort(mergedProperties, ssl);
		String database = getDatabase(mergedProperties, path);
		Map<String, String> additionalProperties = getFireboltCustomProperties(mergedProperties);

		return FireboltProperties.builder().ssl(ssl).sslCertificatePath(sslRootCertificate).sslMode(sslMode).path(path)
				.port(port).database(database).compress(compress).user(user).password(password).host(host)
				.additionalProperties(additionalProperties).account(account).engine(engine)
				.maxConnectionsPerRoute(maxConnectionsPerRoute).timeToLiveMillis(timeToLiveMillis)
				.validateAfterInactivityMillis(validateAfterInactivityMillis).maxConnectionsTotal(maxTotal)
				.maxRetries(maxRetries).clientBufferSize(clientBufferSize).bufferSize(bufferSize)
				.socketTimeoutMillis(socketTimeout).connectionTimeoutMillis(connectionTimeout)
				.keepAliveTimeoutMillis(keepAliveTimeout).tcpKeepInterval(tcpKeepInterval).tcpKeepCount(tcpKeepCount)
				.tcpKeepIdle(tcpKeepIdle).aggressiveCancel(aggressiveCancel).logResultSet(logResultSet).build();
	}

	private static String getHost(Properties properties) {
		String host = getSetting(properties, FireboltSessionProperty.HOST);
		if (StringUtils.isEmpty(host)) {
			throw new IllegalArgumentException("Invalid host: The host is missing or empty");
		} else {
			return host;
		}
	}

	@NonNull
	private static Integer getPort(Properties properties, boolean ssl) {
		Integer port = getSetting(properties, FireboltSessionProperty.PORT);
		if (port == null) {
			port = ssl ? FIREBOLT_SSL_PROXY_PORT : FIREBOLT_NO_SSL_PROXY_PORT;
		}
		return port;
	}

	private static String getDatabase(Properties properties, String path) throws IllegalArgumentException {
		String database = getSetting(properties, FireboltSessionProperty.DATABASE);
		if (StringUtils.isEmpty(database)) {
			if ("/".equals(path)) {
				throw new IllegalArgumentException("A database must be provided");
			} else {
				Matcher m = DB_PATH_PATTERN.matcher(path);
				if (m.matches()) {
					return m.group(1);
				} else {
					throw new IllegalArgumentException(String.format("The database provided is invalid %s", path));
				}
			}
		} else {
			return database;
		}
	}

	private static Map<String, String> getFireboltCustomProperties(Properties properties) {
		return properties.entrySet().stream().filter(entry -> !sessionPropertyKeys.contains(entry.getKey()))
				.collect(Collectors.toMap(e -> (String) e.getKey(), e -> e.getValue().toString(), (x, y) -> y));
	}

	@SuppressWarnings("unchecked")
	private static <T> T getSetting(Properties info, FireboltSessionProperty param) {
		String val = info.getProperty(param.getKey());
		Object defaultValue = param.getDefaultValue();
		Class<?> clazz = param.getClazz();
		if (val == null) {
			String[] aliases = param.getAliases();
			int i = 0;
			while (val == null && i < aliases.length) {
				val = info.getProperty(aliases[i++]);
			}
		}

		if (val == null)
			return (T) defaultValue;
		if (clazz == int.class || clazz == Integer.class) {
			return (T) clazz.cast(Integer.valueOf(val));
		}
		if (clazz == long.class || clazz == Long.class) {
			return (T) clazz.cast(Long.valueOf(val));
		}
		if (clazz == boolean.class || clazz == Boolean.class) {
			boolean boolValue;
			if (StringUtils.isNumeric(val)) {
				boolValue = Integer.parseInt(val) > 0;
			} else {
				boolValue = Boolean.parseBoolean(val);
			}
			return (T) clazz.cast(boolValue);
		}
		return (T) clazz.cast(val);
	}

	private static Properties mergeProperties(Properties... properties) {
		Properties mergedProperties = new Properties();
		for (Properties p : properties) {
			mergedProperties.putAll(p);
		}
		return mergedProperties;
	}

	public static FireboltProperties copy(FireboltProperties properties) {
		return properties.toBuilder().additionalProperties(new HashMap<>(properties.getAdditionalProperties())).build();
	}

	public void addProperty(@NonNull String key, String value) {
		additionalProperties.put(key, value);
	}

	public void addProperty(Pair<String, String> property) {
		this.addProperty(property.getLeft(), property.getRight());
	}
}
