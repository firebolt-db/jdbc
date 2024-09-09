package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static integration.EnvironmentCondition.Attribute.databaseVersion;
import static integration.EnvironmentCondition.Comparison.GE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
class TimeoutTest extends IntegrationTest {
	private static final int MIN_TIME_SECONDS = 350;
	private static final Map<Integer, Long> SERIES_SIZE = Map.of(1, 80000000000L, 2, 600000000000L);
	private long startTime;

	@BeforeEach
	void before() {
		startTime = System.nanoTime();
	}

	@AfterEach
	void after() {
		long endTime = System.nanoTime();
		long elapsedTimeSeconds = (endTime - startTime) / 1_000_000_000;
		log.info("Time elapsed: {} seconds", elapsedTimeSeconds);
		assertTrue(elapsedTimeSeconds > MIN_TIME_SECONDS, format("Test is too short. It took %d but should take at least %d seconds", elapsedTimeSeconds, MIN_TIME_SECONDS));
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.MINUTES)
	@Tag("v1") // generate_series is supported on all available engine of v2
	@Tag("slow")
	void shouldExecuteRequestWithoutTimeoutV1() throws SQLException {
		shouldExecuteRequestWithoutTimeout();
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.MINUTES)
	@EnvironmentCondition(value = "3.33", attribute = databaseVersion, comparison = GE) // generate_series is supported starting from version 3.33 on v2
	@Tag("v2")
	@Tag("slow")
	void shouldExecuteRequestWithoutTimeoutV2() throws SQLException {
		shouldExecuteRequestWithoutTimeout();
	}

	private void shouldExecuteRequestWithoutTimeout() throws SQLException {
		try (Connection con = createConnection(); Statement stmt = con.createStatement()) {
			int infraVersion = ((FireboltConnection)con).getInfraVersion();
			stmt.executeQuery(format("SELECT (max(x) - min(x))/count(x) + avg(x) FROM generate_series(1,%d) r(x)", SERIES_SIZE.get(infraVersion)));
		}
	}
}
