package integration.tests;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import integration.IntegrationTest;
import lombok.CustomLog;

@CustomLog
class TimeoutTest extends IntegrationTest {

	@Test
	@Timeout(value = 7, unit = TimeUnit.MINUTES)
	@Tag("slow")
	void shouldExecuteRequestWithoutTimeout() throws SQLException {
		long startTime = System.nanoTime();
		try (Connection con = this.createConnection(); Statement stmt = con.createStatement()) {
			stmt.executeQuery("SELECT checksum(*) FROM generate_series(1, 100000000000)");
		} finally {
			log.info("Time elapsed: " + (System.nanoTime() - startTime) / 1_000_000_000 + " seconds");
		}
	}
}
