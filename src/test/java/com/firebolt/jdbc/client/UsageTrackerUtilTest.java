package com.firebolt.jdbc.client;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.firebolt.jdbc.client.DriverVersionRetriever.getDriverVersion;
import static com.firebolt.jdbc.client.UsageTrackerUtil.CLIENT_MAP;
import static com.firebolt.jdbc.client.UsageTrackerUtil.DRIVER_MAP;
import static com.firebolt.jdbc.client.UserAgentFormatter.javaVersion;
import static com.firebolt.jdbc.client.UserAgentFormatter.osVersion;
import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UsageTrackerUtilTest {
	private static final String REAL_OS_NAME = System.getProperty("os.name");

	@AfterEach
	void afterEach() {
		System.setProperty("os.name", REAL_OS_NAME);
	}

	private MockedStatic<UsageTrackerUtil> mockClients(Map<String, String> drivers, Map<String, String> clients) {
		return Mockito.mockStatic(UsageTrackerUtil.class, invocation -> {
			Method method = invocation.getMethod();
			if ("getClients".equals(method.getName())) {
				if (invocation.getArgument(1) == DRIVER_MAP) {
					return drivers;
				} else if (invocation.getArgument(1) == CLIENT_MAP) {
					return clients;
				}
				return new HashMap<String, String>();
			} else {
				return invocation.callRealMethod();
			}
		});
	}

	@ParameterizedTest(name = "{0} -> {1}")
	@CsvSource(value = {
			"MacosX, Darwin",
			"Windows, Windows",
			"linux, Linux",
			"HackintoSh, hackintosh"
	}, delimiter = ',')
	void shouldGetUserAgentNoOverride(String osName, String expectedOsInUserAgent) {
		System.setProperty("os.name", osName);
		String result = UsageTrackerUtil.getUserAgentString("", "");
		assertEquals(userAgent("JDBC/%s (Java %s; %s %s; )", getDriverVersion(), javaVersion(), expectedOsInUserAgent, osVersion(), Optional.empty()), result);
	}

	@ParameterizedTest(name = "userClients: {0}")
	@CsvSource(value = {
			"'',''",
			"AwesomeClient:2.0.1,'AwesomeClient/2.0.1 '",
	}, delimiter = ',')
	void shouldGetUserAgentCustomConnectors(String userClients, String expectedPrefix) {
		System.setProperty("os.name", "MacosX");
		String result = UsageTrackerUtil.getUserAgentString("AwesomeDriver:1.0.1,BadConnector:0.1.4", userClients);
		assertEquals(expectedPrefix + userAgent("JDBC/%s (Java %s; %s %s; ) AwesomeDriver/1.0.1 BadConnector/0.1.4", getDriverVersion(), javaVersion(), "Darwin", osVersion(), Optional.empty()), result);
	}

	@ParameterizedTest(name = "{0} -> {1}")
	@CsvSource(value = {
			"BadConnector%0.1.4,''",
			"'',BadConnector%0.1.4",
			"'',BadConnector",
			"BadConnector%0.1.5, BadConnector%0.1.4",
			"VeryBadConnector, BadConnector",
	}, delimiter = ',')
	void shouldIgnoreIncorrectCustomConnectors(String userDrivers, String userClients) {
		System.setProperty("os.name", "MacosX");
		String result = UsageTrackerUtil.getUserAgentString(userDrivers, userClients);
		assertEquals(userAgent("JDBC/%s (Java %s; %s %s; )", getDriverVersion(), javaVersion(), "Darwin", osVersion(), Optional.empty()), result);
	}

	@Test
	void shouldDetectConnectorStack() {
		userAgentWithConnectors(new HashMap<>(Map.of("ConnA", "1.2.0", "ConnB", "3.0.4")), new HashMap<>(),"", "", userAgent("JDBC/%s (Java %s; %s %s; ) ConnA/1.2.0 ConnB/3.0.4"));
	}

	@Test
	void shouldWorkWithNoConnectorsDetected() {
		userAgentWithConnectors(new HashMap<>(), new HashMap<>(), "", "", userAgent("JDBC/%s (Java %s; %s %s; )"));
	}

	private void userAgentWithConnectors(Map<String, String> drivers, Map<String, String> connectors, String userDrivers, String userClients, String expectedUserAgent) {
		//noinspection unused required by syntax of try-with-resource
		try (MockedStatic<UsageTrackerUtil> mock = mockClients(drivers, connectors)) {
			String result = UsageTrackerUtil.getUserAgentString(userDrivers, userClients);
			assertEquals(expectedUserAgent, result);
		}
	}

	@Test
	void shouldOverrideConnectors() {
		userAgentWithConnectors(new HashMap<>(), new HashMap<>(Map.of("ConnA", "1.2.0", "ConnB", "3.0.4")), "", "ConnA:2.0.1,ConnC:1.1.1", userAgent("ConnA/2.0.1 ConnB/3.0.4 ConnC/1.1.1 JDBC/%s (Java %s; %s %s; )"));
	}

	@Test
	void shouldParseStackUnknownValues() {
		StackTraceElement[] stack;
		StackTraceElement mockedStack1;
		StackTraceElement mockedStack2;
		Map<String, String> clients;

		stack = new StackTraceElement[0];
		clients = UsageTrackerUtil.getClients(stack, CLIENT_MAP);
		assertTrue(clients.isEmpty());

		stack = new StackTraceElement[2];
		mockedStack1 = mock(StackTraceElement.class);
		when(mockedStack1.getClassName()).thenReturn("org.dummy.my");
		stack[0] = mockedStack1;
		mockedStack2 = mock(StackTraceElement.class);
		when(mockedStack2.getClassName()).thenReturn("com.dummy.my");
		stack[1] = mockedStack2;
		clients = UsageTrackerUtil.getClients(stack, CLIENT_MAP);
		assertTrue(clients.isEmpty());
	}

	@Test
	void shouldParseDetectStack() {
		StackTraceElement[] stack;
		StackTraceElement mockedStack1;
		StackTraceElement mockedStack2;
		Map<String, String> clients;

		stack = new StackTraceElement[2];
		mockedStack1 = mock(StackTraceElement.class);
		when(mockedStack1.getClassName()).thenReturn("com.tableau.my.value");
		stack[0] = mockedStack1;
		mockedStack2 = mock(StackTraceElement.class);
		when(mockedStack2.getClassName()).thenReturn("com.dummy.my");
		stack[1] = mockedStack2;
		Map<String, String> expected = new HashMap<>();
		expected.put("Tableau", "1.0.2");

		// Can't mock Class so have to mock the outer function
		//noinspection unused required by syntax of try-with-resource
		try (MockedStatic<UsageTrackerUtil> fooUtilsMocked = Mockito.mockStatic(UsageTrackerUtil.class, invocation -> {
			Method method = invocation.getMethod();
			if ("getVersionForClass".equals(method.getName())) {
				return "1.0.2";
			} else {
				return invocation.callRealMethod();
			}
		})) {
			clients = UsageTrackerUtil.getClients(stack, CLIENT_MAP);
			assertFalse(clients.isEmpty());
			assertEquals(expected, clients);
		}
	}

	@ParameterizedTest(name = "{1}")
	@CsvSource(value = {
			"98,less than 100,ConnA/1.1.1 ConnB/2.0.1",
			"99,equals to 100,ConnA/1.1.1 ConnB/2.0.1",
			"100, greater than 100,ConnA/1.2.0 ConnB/3.0.4",
			"101, seriously greater than 100,ConnA/1.2.0 ConnB/3.0.4"
	}, delimiter = ',')
	void shouldNotOverrideConnectorsIfMoreThanSeveralNameVersionPairsAreSpecified(int n, String name, String expectedUserAgentPrefix) {
		String connectorsInfo = "ConnB:2.0.1," + String.join(",", Collections.nCopies(n, "ConnA:1.1.1"));
		Map<String, String> connectors = new HashMap<>(Map.of("ConnA", "1.2.0", "ConnB", "3.0.4"));
		userAgentWithConnectors(new HashMap<>(), connectors, "", connectorsInfo, userAgent(expectedUserAgentPrefix + " JDBC/%s (Java %s; %s %s; )"));
	}
}
