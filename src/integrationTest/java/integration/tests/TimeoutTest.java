package integration.tests;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import integration.IntegrationTest;
import lombok.CustomLog;

import static org.junit.jupiter.api.Assertions.*;

@CustomLog
class TimeoutTest extends IntegrationTest {

	@Test
	@Timeout(value = 120, unit = TimeUnit.MINUTES)
	void shouldExecuteRequestWithoutTimeout() {
		long startTime = System.nanoTime();
		try (Connection con = this.createConnection(); Statement stmt = con.createStatement()) {
			this.setParam(con, "use_standard_sql", "0");
			this.setParam(con, "advanced_mode", "1");
			int fiftyMinutes = 50 * 60;
			ResultSet rs = stmt.executeQuery(String.format("SELECT sleepEachRow(1) from numbers(%d)", fiftyMinutes));
			for (int i = 0 ; i < fiftyMinutes; i++) {
				rs.next();
				assertEquals(false, rs.getObject(1));
			}
			assertFalse(rs.next());
		} catch (Exception e) {
			log.error("Error", e);
			fail();
		}
		log.info("Time elapsed: " + (System.nanoTime() - startTime) / 1_000_000_000 + " seconds");
	}
}
