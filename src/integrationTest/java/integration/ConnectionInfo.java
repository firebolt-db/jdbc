package integration;

import lombok.Value;

@Value
public class ConnectionInfo {
	private static ConnectionInfo INSTANCE;
	String password;
	String user;
	String api;
	String database;

	private ConnectionInfo() {
		password = System.getProperty("password").replace("\"", "");
		user = System.getProperty("user").replace("\"", "");
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
