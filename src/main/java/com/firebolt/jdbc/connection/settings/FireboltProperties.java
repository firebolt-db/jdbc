package com.firebolt.jdbc.connection.settings;

import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

@Value
@Builder(toBuilder = true)
@CustomLog
public class FireboltProperties {

	public static final String SYSTEM_ENGINE_NAME = "system";
	private static final Pattern DB_PATH_PATTERN = Pattern.compile("([a-zA-Z0-9_*\\-]+)");
	private static final int FIREBOLT_SSL_PROXY_PORT = 443;
	private static final int FIREBOLT_NO_SSL_PROXY_PORT = 9090;

	private static final Set<String> sessionPropertyKeys = Arrays.stream(FireboltSessionProperty.values())
			.map(property -> {
				List<String> keys = new ArrayList<>();
				keys.add(property.getKey());
				keys.addAll(Arrays.asList(property.getAliases()));
				return keys;
			}).flatMap(List::stream).collect(Collectors.toSet());

	int keepAliveTimeoutMillis;
	int maxConnectionsTotal;
	int maxRetries;
	int bufferSize;
	int clientBufferSize;
	int socketTimeoutMillis;
	int connectionTimeoutMillis;
	Integer port;
	String host;
	String database;
	String path;
	boolean ssl;
	String sslCertificatePath;
	String sslMode;
	boolean compress;
	String principal;
	String secret;
	String engine;
	String account;
	String accountId;
	Integer tcpKeepIdle;
	Integer tcpKeepCount;
	Integer tcpKeepInterval;
	boolean logResultSet;
	boolean systemEngine;
	String environment;
	String userDrivers;
	String userClients;

	@Builder.Default
	Map<String, String> additionalProperties = new HashMap<>();

	public static FireboltProperties of(Properties... properties) {
		Properties mergedProperties = mergeProperties(properties);
		boolean ssl = getSetting(mergedProperties, FireboltSessionProperty.SSL);
		String sslRootCertificate = getSetting(mergedProperties, FireboltSessionProperty.SSL_CERTIFICATE_PATH);
		String sslMode = getSetting(mergedProperties, FireboltSessionProperty.SSL_MODE);
		String principal = getSetting(mergedProperties, FireboltSessionProperty.CLIENT_ID);
		String secret = getSetting(mergedProperties, FireboltSessionProperty.CLIENT_SECRET);
		String path = getSetting(mergedProperties, FireboltSessionProperty.PATH);
		String database = getDatabase(mergedProperties, path);
		String engine = getEngine(mergedProperties, database);
		boolean isSystemEngine = isSystemEngine(engine);
		boolean compress = ((Boolean) getSetting(mergedProperties, FireboltSessionProperty.COMPRESS))
				&& !isSystemEngine;
		String account = getSetting(mergedProperties, FireboltSessionProperty.ACCOUNT);
		int keepAliveMillis = getSetting(mergedProperties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT_MILLIS);
		int maxTotal = getSetting(mergedProperties, FireboltSessionProperty.MAX_CONNECTIONS_TOTAL);
		int maxRetries = getSetting(mergedProperties, FireboltSessionProperty.MAX_RETRIES);
		int bufferSize = getSetting(mergedProperties, FireboltSessionProperty.BUFFER_SIZE);
		int socketTimeout = getSetting(mergedProperties, FireboltSessionProperty.SOCKET_TIMEOUT_MILLIS);
		int connectionTimeout = getSetting(mergedProperties, FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS);
		int tcpKeepInterval = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_INTERVAL);
		int tcpKeepIdle = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_IDLE);
		int tcpKeepCount = getSetting(mergedProperties, FireboltSessionProperty.TCP_KEEP_COUNT);
		boolean logResultSet = getSetting(mergedProperties, FireboltSessionProperty.LOG_RESULT_SET);
		String configuredEnvironment = getSetting(mergedProperties, FireboltSessionProperty.ENVIRONMENT);
		String driverVersions = getSetting(mergedProperties, FireboltSessionProperty.USER_DRIVERS);
		String clientVersions = getSetting(mergedProperties, FireboltSessionProperty.USER_CLIENTS);

		String environment = getEnvironment(configuredEnvironment, mergedProperties);
		String host = getHost(configuredEnvironment, mergedProperties);
		Integer port = getPort(mergedProperties, ssl);

		Map<String, String> additionalProperties = getFireboltCustomProperties(mergedProperties);

		return FireboltProperties.builder().ssl(ssl).sslCertificatePath(sslRootCertificate).sslMode(sslMode).path(path)
				.port(port).database(database).compress(compress).principal(principal).secret(secret).host(host)
				.additionalProperties(additionalProperties).account(account).engine(engine)
				.keepAliveTimeoutMillis(keepAliveMillis).maxConnectionsTotal(maxTotal).maxRetries(maxRetries)
				.bufferSize(bufferSize).socketTimeoutMillis(socketTimeout).connectionTimeoutMillis(connectionTimeout)
				.tcpKeepInterval(tcpKeepInterval).tcpKeepCount(tcpKeepCount).tcpKeepIdle(tcpKeepIdle)
				.logResultSet(logResultSet).systemEngine(isSystemEngine)
				.environment(environment)
				.userDrivers(driverVersions)
				.userClients(clientVersions)
				.build();
	}

	private static String getEngine(Properties mergedProperties, String database) {
		String engine = getSetting(mergedProperties, FireboltSessionProperty.ENGINE);
		if (StringUtils.isEmpty(engine) && StringUtils.isEmpty(database)) {
			return SYSTEM_ENGINE_NAME;
		} else {
			return engine;
		}
	}

	private static String getHost(String environment, Properties properties ) {
		String host = getSetting(properties, FireboltSessionProperty.HOST);
		if (StringUtils.isEmpty(host)) {
			return format("api.%s.firebolt.io", environment);
		} else {
			return host;
		}
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

	private static boolean isSystemEngine(String engine) {
		return StringUtils.equalsIgnoreCase(SYSTEM_ENGINE_NAME, engine);
	}

	public void addProperty(@NonNull String key, String value) {
		additionalProperties.put(key, value);
	}

	public void addProperty(Pair<String, String> property) {
		this.addProperty(property.getLeft(), property.getRight());
	}
}
