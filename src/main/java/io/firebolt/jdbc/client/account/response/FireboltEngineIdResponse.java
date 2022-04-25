package io.firebolt.jdbc.client.account.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FireboltEngineIdResponse {
    @JsonProperty("engine_id")
    Engine engine;

    @Value
    @Builder
    public static class Engine {
        @JsonProperty("engine_id")
        String engineId;
    }
}