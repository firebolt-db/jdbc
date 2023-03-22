package com.firebolt.jdbc.client.gateway;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;


@Value
@AllArgsConstructor
@Builder
public class GatewayUrlResponse {
    @JsonProperty("engineUrl")
    String engineUrl;
}
