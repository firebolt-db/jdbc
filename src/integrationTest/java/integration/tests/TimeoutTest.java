package integration.tests;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class TimeoutTest extends IntegrationTest {

	@Test
	void shouldExecuteRequestWithoutTimeout() {
		long startTime = System.nanoTime();
		long endTime = System.nanoTime();
		try (Connection con = this.createConnection(); Statement stmt = con.createStatement()) {
			this.setParam(con, "use_standard_sql", "0");
			this.setParam(con, "advanced_mode", "1");
			stmt.executeQuery("SELECT sleepEachRow(1) from numbers(360)");
		} catch (Exception e) {
			log.error("Error", e);
			fail();
		}
		log.info("Time elapsed: " + (endTime - startTime) / 1_000_000_000 + " seconds");
	}
}
