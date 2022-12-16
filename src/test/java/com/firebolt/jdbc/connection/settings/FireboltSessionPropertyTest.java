package com.firebolt.jdbc.connection.settings;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

class FireboltSessionPropertyTest {

	@Test
	void shouldNotReturnAnyDeprecatedProperty() {
		List<FireboltSessionProperty> properties = FireboltSessionProperty.getNonDeprecatedProperties();
		List<FireboltSessionProperty> deprecatedProperties = Arrays.asList(TIME_TO_LIVE_MILLIS,
				MAX_CONNECTIONS_PER_ROUTE, USE_PATH_AS_DB, USE_PATH_AS_DB, USE_CONNECTION_POOL,
				VALIDATE_AFTER_INACTIVITY_MILLIS);
		assertTrue(Collections.disjoint(properties, deprecatedProperties));
	}
}