package io.firebolt.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProjectVersionUtilTest {

  @Test
  void getMajorVersion() {
    assertEquals(2, ProjectVersionUtil.getMajorVersion());
  }

  @Test
  void getMinorVersion() {
    assertEquals(5, ProjectVersionUtil.getMinorVersion());
  }

  @Test
  void getProjectVersion() {
    assertEquals("2.5-SNAPSHOT", ProjectVersionUtil.getProjectVersion());
  }
}
