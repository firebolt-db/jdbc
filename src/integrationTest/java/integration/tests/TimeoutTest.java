package integration.tests;

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
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
class TimeoutTest extends IntegrationTest {
	private static final int MIN_TIME_SECONDS = 350;
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
	@Tag("slow")
	void shouldExecuteRequestWithoutTimeout() throws SQLException {
		try (Connection con = createConnection(); Statement stmt = con.createStatement()) {
			stmt.executeQuery("SELECT checksum(*) FROM generate_series(1, 300000000000)");
		}
	}
}
