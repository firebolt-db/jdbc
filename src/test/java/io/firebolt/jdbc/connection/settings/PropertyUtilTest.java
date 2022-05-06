package io.firebolt.jdbc.connection.settings;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PropertyUtilTest {

  @Mock private PropertiesToFireboltPropertiesTransformer propertiesToFireboltPropertiesTransformer;

  @Test
  void shouldExtractAllProperties() {
    String uri =
        "jdbc:firebolt://api.dev.firebolt.io:123/Tutorial_11_04?use_standard_sql=0&account=firebolt";
    Properties connexionSettings = new Properties();
    connexionSettings.put("property", "value");

    Properties expectedProperties = new Properties();
    expectedProperties.put("path", "/Tutorial_11_04");
    expectedProperties.put("host", "api.dev.firebolt.io");
    expectedProperties.put("port", 123);
    expectedProperties.put("use_standard_sql", "0");
    expectedProperties.put("account", "firebolt");
    expectedProperties.put("property", "value");

    when(propertiesToFireboltPropertiesTransformer.apply(expectedProperties))
        .thenReturn(FireboltProperties.builder().build());
    PropertyUtil.extractFireboltProperties(
        uri, connexionSettings, propertiesToFireboltPropertiesTransformer);
    verify(propertiesToFireboltPropertiesTransformer).apply(expectedProperties);
  }
}
