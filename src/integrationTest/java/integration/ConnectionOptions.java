package integration;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ConnectionOptions {

    // should only be used for cloud connections (either v1 or v2)
    private String engine;

    private String database;

    @Builder.Default
    private Map<String,String> connectionParams = new HashMap<>();

    // should only be used for core connection
    @Builder.Default
    private String url = "http://localhost:3473";

}
