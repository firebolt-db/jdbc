package com.firebolt.jdbc.client.query.response.s3;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Getter
@Builder
@Data
public class AwsInfo {

    private String region;
    private String keyId;
    private String keySecret;
    private String sessionToken;
}
