package integration.tests;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.io.Resources;

import lombok.SneakyThrows;

public abstract class IntegrationTest {
	protected Connection createConnection() throws SQLException {
		return DriverManager.getConnection(
				"jdbc:firebolt://" + integration.ConnectionInfo.getInstance().getApi() + "/"
						+ integration.ConnectionInfo.getInstance().getDatabase(),
				integration.ConnectionInfo.getInstance().getUser(),
				integration.ConnectionInfo.getInstance().getPassword());
	}

	protected void setParam(Connection connection, String name, String value) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(String.format("set %s = %s", name, value));
		}
	}

	@SneakyThrows
	protected void executeStatementFromFile(String path) {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			String createTable = Resources.toString(IntegrationTest.class.getResource(path), StandardCharsets.UTF_8);
			statement.execute(createTable);
		}
	}

}
