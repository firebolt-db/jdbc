package com.firebolt.jdbc.client;

import com.firebolt.jdbc.CloseableUtil;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;

@Slf4j
@UtilityClass
public class ClientUtil {
    void closeQuietly(Response response) {
        if (response != null && response.body() != null) {
            CloseableUtil.close(response.body());
        }
    }
}