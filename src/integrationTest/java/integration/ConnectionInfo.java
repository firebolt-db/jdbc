package integration;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.stream.Collectors.joining;

public class ConnectionInfo {
	private static final String JDBC_URL_PREFIX = "jdbc:firebolt:";
	private static volatile ConnectionInfo INSTANCE;
	// principal and secret are used here instead of client_id and client_secret respectively as more common term also used in java security API.
	private final String principal;
	private final String secret;
	private final String env;
	private final String database;
	private final String account;
	private final String engine;

	private ConnectionInfo() {
		this(
				getTrimmedProperty("client_id", "user"),
				getTrimmedProperty("client_secret", "password"),
				getProperty("env"),
				getProperty("db"),
				getProperty("account"),
				getProperty("engine")
		);
	}

	public ConnectionInfo(String principal, String secret, String env, String database, String account, String engine) {
		this.principal = principal;
		this.secret = secret;
		this.env = env;
		this.database = database;
		this.account = account;
		this.engine = engine;
	}

	public static ConnectionInfo getInstance() {
		if (INSTANCE == null) {
			synchronized (ConnectionInfo.class) {
				if (INSTANCE == null) {
					INSTANCE = new ConnectionInfo();
				}
			}
		}
		return INSTANCE;
	}

	private static String getTrimmedProperty(String name, String alias) {
		return Optional.ofNullable(getProperty(name, getProperty(alias))).map(u -> u.replace("\"", "")).orElse(null);
	}

	public String getPrincipal() {
		return principal;
	}

	public String getSecret() {
		return secret;
	}

	public String getEnv() {
		return env;
	}

	public String getDatabase() {
		return database;
	}

	public String getAccount() {
		return account;
	}

	public String getEngine() {
		return engine;
	}

	public String toJdbcUrl() {
		String params = Stream.of(param("env", env), param("engine", engine), param("account", account)).filter(Objects::nonNull).collect(joining("&"));
		if (!params.isEmpty()) {
			params = "?" + params;
		}
		return JDBC_URL_PREFIX + database + params;
	}

	private String param(String name, String value) {
		return value == null ? null : format("%s=%s", name, value);
	}
}
