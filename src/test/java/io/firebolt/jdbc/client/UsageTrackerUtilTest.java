package io.firebolt.jdbc.client;

import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.ProjectVersionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UsageTrackerUtil.class, Class.class})
public class UsageTrackerUtilTest {

  private static MockedStatic<ProjectVersionUtil> mockedProjectVersionUtil;

  @BeforeAll
  static void init() {
    mockedProjectVersionUtil = mockStatic(ProjectVersionUtil.class);
    mockedProjectVersionUtil.when(ProjectVersionUtil::getProjectVersion).thenReturn("1.0-TEST");
    System.setProperty("java.version", "8.0.1");
    System.setProperty("os.version", "10.1");
  }

  @AfterAll
  public static void close() {
    mockedProjectVersionUtil.close();
  }

  @Test
  void shouldGetUserAgentNoOverride() {
    System.setProperty("os.name", "MacosX");
    String result = UsageTrackerUtil.getUserAgentString("");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ", result);
    System.setProperty("os.name", "Windows");
    result = UsageTrackerUtil.getUserAgentString("");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; Windows 10.1; ) ", result);
    System.setProperty("os.name", "linux");
    result = UsageTrackerUtil.getUserAgentString("");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; Linux 10.1; ) ", result);
    System.setProperty("os.name", "HackintoSh");
    result = UsageTrackerUtil.getUserAgentString("");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; hackintosh 10.1; ) ", result);
  }

  @Test
  void shouldGetUserAgentCustomConnectors() {
    System.setProperty("os.name", "MacosX");
    String result =
        UsageTrackerUtil.getUserAgentString("AwesomeConnector:1.0.1,BadConnector:0.1.4");
    assertEquals(
        "JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) BadConnector/0.1.4 AwesomeConnector/1.0.1",
        result);
  }

  @Test
  void shouldIgnoreIncorrectCustomConnectors() {
    System.setProperty("os.name", "MacosX");
    String result = UsageTrackerUtil.getUserAgentString("BadConnector%0.1.4");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ", result);
    result = UsageTrackerUtil.getUserAgentString("BadConnector");
    assertEquals("JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ", result);
  }

  @Test
  void shouldDetectConnectorStack() {
    System.setProperty("os.name", "MacosX");
    Map<String, String> connectors =
        ImmutableMap.of(
            "ConnA", "1.2.0",
            "ConnB", "3.0.4");
    try (MockedStatic<UsageTrackerUtil> fooUtilsMocked =
        Mockito.mockStatic(
            UsageTrackerUtil.class,
            invocation -> {
              Method method = invocation.getMethod();
              if ("getClients".equals(method.getName())) {
                return connectors;
              } else {
                return invocation.callRealMethod();
              }
            })) {
      String result = UsageTrackerUtil.getUserAgentString("");
      assertEquals("JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.2.0 ConnB/3.0.4", result);
    }
  }

  @Test
  void shouldWorkWithNoConnectorsDetected() {
    System.setProperty("os.name", "MacosX");
    Map<String, String> connectors = new HashMap<String, String>();
    try (MockedStatic<UsageTrackerUtil> fooUtilsMocked =
        Mockito.mockStatic(
            UsageTrackerUtil.class,
            invocation -> {
              Method method = invocation.getMethod();
              if ("getClients".equals(method.getName())) {
                return connectors;
              } else {
                return invocation.callRealMethod();
              }
            })) {
      String result = UsageTrackerUtil.getUserAgentString("");
      assertEquals("JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ", result);
    }
  }

  @Test
  void shouldOverrideConnectors() {
    System.setProperty("os.name", "MacosX");
    Map<String, String> connectors = new HashMap<>();
    connectors.put("ConnA", "1.2.0");
    connectors.put("ConnB", "3.0.4");
    try (MockedStatic<UsageTrackerUtil> fooUtilsMocked =
        Mockito.mockStatic(
            UsageTrackerUtil.class,
            invocation -> {
              Method method = invocation.getMethod();
              if ("getClients".equals(method.getName())) {
                return connectors;
              } else {
                return invocation.callRealMethod();
              }
            })) {
      String result = UsageTrackerUtil.getUserAgentString("ConnA:2.0.1,ConnC:1.1.1");
      assertEquals(
          "JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/2.0.1 ConnB/3.0.4 ConnC/1.1.1", result);
    }
  }

  @Test
  void shouldParseStackUnknownValues() {
    StackTraceElement[] stack;
    StackTraceElement mockedStack1;
    StackTraceElement mockedStack2;
    Map<String, String> clients;

    stack = new StackTraceElement[0];
    clients = UsageTrackerUtil.getClients(stack);
    assertTrue(clients.isEmpty());

    stack = new StackTraceElement[2];
    mockedStack1 = mock(StackTraceElement.class);
    when(mockedStack1.getClassName()).thenReturn("org.dummy.my");
    stack[0] = mockedStack1;
    mockedStack2 = mock(StackTraceElement.class);
    when(mockedStack2.getClassName()).thenReturn("com.dummy.my");
    stack[1] = mockedStack2;
    clients = UsageTrackerUtil.getClients(stack);
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
    try (MockedStatic<UsageTrackerUtil> fooUtilsMocked =
        Mockito.mockStatic(
            UsageTrackerUtil.class,
            invocation -> {
              Method method = invocation.getMethod();
              if ("getVersionForClass".equals(method.getName())) {
                return "1.0.2";
              } else {
                return invocation.callRealMethod();
              }
            })) {
      clients = UsageTrackerUtil.getClients(stack);
      assertFalse(clients.isEmpty());
      assertThat(clients, is(expected));
    }
  }
}
