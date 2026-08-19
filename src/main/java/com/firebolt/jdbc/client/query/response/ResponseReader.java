package com.firebolt.jdbc.client.query.response;

import java.io.InputStream;
import okhttp3.Response;

/**
 * This repsonse reader knows how to transform the response from the http call to firebolt into an input stream
 */
@FunctionalInterface
public interface ResponseReader {

    /**
     * converts the http response into an input stream
     * @param response
     * @return
     */
    InputStream read(Response response);

}
