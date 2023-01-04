package integration;

import lombok.Value;

import java.util.Optional;

@Value
public class ConnectionInfo {
	private static ConnectionInfo INSTANCE;
	String password;
	String user;
	String api;
	String database;

	private ConnectionInfo() {
		password = Optional.ofNullable(System.getProperty("password")).map(p -> p.replace("\"", "")).orElse(null);
		user = Optional.ofNullable(System.getProperty("user")).map(u -> u.replace("\"", "")).orElse(null);
		api = System.getProperty("api");
		database = System.getProperty("db");
	}

	public static ConnectionInfo getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ConnectionInfo();
		}
		return INSTANCE;
	}

}
