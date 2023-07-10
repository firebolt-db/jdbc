package integration;

import java.util.Optional;

import static java.lang.System.getProperty;

public class ConnectionInfo {
	private static volatile ConnectionInfo INSTANCE;
	// principal and secret are used here instead of client_id and client_secret respectively as more common term also used in java security API.
	private final String principal;
	private final String secret;
	private final String env;
	private final String database;
	private final String account;
	private final String engine;

	private ConnectionInfo() {
		principal = Optional.ofNullable(getProperty("client_id", getProperty("user"))).map(u -> u.replace("\"", "")).orElse(null);
		secret = Optional.ofNullable(getProperty("client_secret", getProperty("password"))).map(p -> p.replace("\"", "")).orElse(null);
		env = getProperty("env");
		database = getProperty("db");
		account = getProperty("account");
		engine = getProperty("engine");
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
}
