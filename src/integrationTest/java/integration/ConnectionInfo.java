package integration;

import lombok.Value;

import java.util.Optional;

@Value
public class ConnectionInfo {
	private static ConnectionInfo INSTANCE;
	String password;
	String user;
	String env;
	String database;
	String account;

	private ConnectionInfo() {
		password = Optional.ofNullable(System.getProperty("password")).map(p -> p.replace("\"", "")).orElse(null);
		user = Optional.ofNullable(System.getProperty("user")).map(u -> u.replace("\"", "")).orElse(null);
		env = System.getProperty("env");
		database = System.getProperty("db");
		account = System.getProperty("account");
	}

	public static ConnectionInfo getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ConnectionInfo();
		}
		return INSTANCE;
	}

}
