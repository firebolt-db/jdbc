package com.firebolt.jdbc.connection.settings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.firebolt.jdbc.util.PropertyUtil.mergeProperties;
import static java.lang.String.format;

@Getter
@ToString
@AllArgsConstructor
@EqualsAndHashCode
@Builder(toBuilder = true)
@CustomLog
public class FireboltProperties {

	private static final Pattern DB_PATH_PATTERN = Pattern.compile("/?([a-zA-Z0-9_*\\-]+)");
	private static final int FIREBOLT_SSL_PROXY_PORT = 443;
	private static final int FIREBOLT_NO_SSL_PROXY_PORT = 9090;

	private static final Set<String> sessionPropertyKeys = Arrays.stream(FireboltSessionProperty.values())
			.map(property -> {
				List<String> keys = new ArrayList<>();
				keys.add(property.getKey());
				keys.addAll(Arrays.asList(property.getAliases()));
				return keys;
			}).flatMap(List::stream).collect(Collectors.toSet());

	private final int keepAliveTimeoutMillis;
	private final int maxConnectionsTotal;
	private final int maxRetries;
	private final int bufferSize;
	private final int socketTimeoutMillis;
	private final int connectionTimeoutMillis;
	private final Integer port;
	private final String host;
	private String database; // updatable using use statement
	private final String path;
	private final boolean ssl;
	private final String sslCertificatePath;
	private final String sslMode;
	private final boolean compress;
	private final String principal;
	private final String secret;
	private String engine; // updatable using use statement
	private final String account;
	private final String accountId;
	private final int tcpKeepIdle;
	private final int tcpKeepCount;
	private final int tcpKeepInterval;
	private final boolean logResultSet;
	private boolean systemEngine;
	private final String environment;
	private final String userDrivers;
	private final String userClients;
	private final String accessToken;
	@Builder.Default
	Map<String, String> additionalProperties = new HashMap<>();

	public FireboltProperties(Properties[] allProperties) {
		this(mergeProperties(allProperties));
	}

	public FireboltProperties(Properties properties) {
		ssl = getSetting(properties, FireboltSessionProperty.SSL);
		sslCertificatePath = getSetting(properties, FireboltSessionProperty.SSL_CERTIFICATE_PATH);
		sslMode = getSetting(properties, FireboltSessionProperty.SSL_MODE);
		principal = getSetting(properties, FireboltSessionProperty.CLIENT_ID);
		secret = getSetting(properties, FireboltSessionProperty.CLIENT_SECRET);
		path = getSetting(properties, FireboltSessionProperty.PATH);
		database = getDatabase(properties, path);
		engine = getEngine(properties);
		systemEngine = isSystemEngine(engine);
		compress = ((Boolean) getSetting(properties, FireboltSessionProperty.COMPRESS)) && !systemEngine;
		account = getSetting(properties, FireboltSessionProperty.ACCOUNT);
		accountId = getSetting(properties, FireboltSessionProperty.ACCOUNT_ID);
		keepAliveTimeoutMillis = getSetting(properties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT_MILLIS);
		maxConnectionsTotal = getSetting(properties, FireboltSessionProperty.MAX_CONNECTIONS_TOTAL);
		maxRetries = getSetting(properties, FireboltSessionProperty.MAX_RETRIES);
		bufferSize = getSetting(properties, FireboltSessionProperty.BUFFER_SIZE);
		socketTimeoutMillis = getSetting(properties, FireboltSessionProperty.SOCKET_TIMEOUT_MILLIS);
		connectionTimeoutMillis = getSetting(properties, FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS);
		tcpKeepInterval = getSetting(properties, FireboltSessionProperty.TCP_KEEP_INTERVAL);
		tcpKeepIdle = getSetting(properties, FireboltSessionProperty.TCP_KEEP_IDLE);
		tcpKeepCount = getSetting(properties, FireboltSessionProperty.TCP_KEEP_COUNT);
		logResultSet = getSetting(properties, FireboltSessionProperty.LOG_RESULT_SET);
		String configuredEnvironment = getSetting(properties, FireboltSessionProperty.ENVIRONMENT);
		userDrivers = getSetting(properties, FireboltSessionProperty.USER_DRIVERS);
		userClients = getSetting(properties, FireboltSessionProperty.USER_CLIENTS);

		environment = getEnvironment(configuredEnvironment, properties);
		host = getHost(configuredEnvironment, properties);
		port = getPort(properties, ssl);
		accessToken =  getSetting(properties, FireboltSessionProperty.ACCESS_TOKEN);

		additionalProperties = new HashMap<>(getFireboltCustomProperties(properties)); // wrap with HashMap to make these properties updatable
	}

	private static String getEngine(Properties mergedProperties) {
		return getSetting(mergedProperties, FireboltSessionProperty.ENGINE);
	}

	private static String getHost(String environment, Properties properties ) {
		String host = getSetting(properties, FireboltSessionProperty.HOST);
		return host == null || host.isEmpty() ? format("api.%s.firebolt.io", environment) : host;
	}

	/**
	 * Discovers environment name from host if it matches pattern {@code api.ENV.firebolt.io}
	 * @param environment - the environment from properties or default value as defined in {@link FireboltSessionProperty#ENVIRONMENT}
	 * @param properties - configuration properties
	 * @return the environment value
	 * @throws IllegalStateException if environment extracted from host is not equal to given one.
	 */
	private static String getEnvironment(String environment, @NotNull Properties properties) {
		Pattern environmentalHost = Pattern.compile("api\\.(.+?)\\.firebolt\\.io");
		String envFromProps = Stream.concat(Stream.of(FireboltSessionProperty.ENVIRONMENT.getKey()), Stream.of(FireboltSessionProperty.ENVIRONMENT.getAliases()))
				.map(properties::getProperty)
				.filter(Objects::nonNull).findFirst()
				.orElse(null);
		String envFromHost = null;
		String host = getSetting(properties, FireboltSessionProperty.HOST);
		if (host != null) {
			Matcher m = environmentalHost.matcher(host);
			if (m.find() && m.group(1) != null) {
				envFromHost = m.group(1);
			}
		}
		if (envFromHost != null) {
			if (envFromProps == null) {
				return envFromHost;
			}
			if (!Objects.equals(environment, envFromHost)) {
				throw new IllegalStateException(format("Environment %s does not match host %s", environment, host));
			}
		}
		return environment;
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
			if ("/".equals(path) || StringUtils.isEmpty(path)) {
				return null;
			} else {
				Matcher m = DB_PATH_PATTERN.matcher(path);
				if (m.matches()) {
					return m.group(1);
				} else {
					throw new IllegalArgumentException(format("The database provided is invalid %s", path));
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

	public static FireboltProperties copy(FireboltProperties properties) {
		return properties.toBuilder().additionalProperties(new HashMap<>(properties.getAdditionalProperties())).build();
	}

	private static boolean isSystemEngine(String engine) {
		return engine == null;
	}

	public void addProperty(@NonNull String key, String value) {
		// This a bad patch but there is nothing to do right now. We will refactor this class and make solution more generic
		switch (key) {
			case "database": database = value; break;
			case "engine": engine = value; break;
			default: additionalProperties.put(key, value);
		}
	}

	public void addProperty(Pair<String, String> property) {
		addProperty(property.getLeft(), property.getRight());
	}

	public String getHttpConnectionUrl() {
		String hostAndPort = host + (port == null ? "" : ":" + port);
		String protocol = isSsl() ? "https://" : "http://";
		return protocol + hostAndPort;
	}
}
