package io.firebolt.jdbc.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FireboltQueryServiceTest {

  @Test
  void shouldExtractAdditionalProperties() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_query=1";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_query", "1")),
        fireboltQueryService.extractAdditionalProperties(query));
  }

  @Test
  void shouldTrimSpacesWhenExtractingAdditionalProperties() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_query= 1 ";
    assertEquals(
            Optional.of(new ImmutablePair<>("my_custom_query", "1")),
            fireboltQueryService.extractAdditionalProperties(query));
  }

  @Test
  void shouldReturnEmptyPropertyWhenExtractingPropertiesWithOnlySpacesAndQuotes() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_char='  '";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_char", StringUtils.EMPTY)),
        fireboltQueryService.extractAdditionalProperties(query));
  }

  @Test
  void shouldReturnEmptyPropertyWhenExtractingAdditionalPropertiesWithASpace() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_char= ";
    assertEquals(
            Optional.of(new ImmutablePair<>("my_custom_char", StringUtils.EMPTY)),
            fireboltQueryService.extractAdditionalProperties(query));
  }

  @Test
  void shouldReturnEmptyPropertyWhenExtractingAdditionalPropertiesWithEmpty() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_char=";
    assertEquals(
            Optional.of(new ImmutablePair<>("my_custom_char", StringUtils.EMPTY)),
            fireboltQueryService.extractAdditionalProperties(query));
  }

  @Test
  void shouldKeepCapitalLettersWhenExtractingProperties() {
    FireboltQueryService fireboltQueryService = new FireboltQueryService(null);

    String query = "set my_custom_char=     myValue1  ";
    assertEquals(
            Optional.of(new ImmutablePair<>("my_custom_char", "myValue1")),
            fireboltQueryService.extractAdditionalProperties(query));
  }
}