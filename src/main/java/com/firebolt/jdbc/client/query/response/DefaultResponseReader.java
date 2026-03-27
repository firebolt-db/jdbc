package com.firebolt.jdbc.client.query.response;

import java.io.InputStream;
import okhttp3.Response;
import okhttp3.ResponseBody;

import static java.util.Optional.ofNullable;

/**
 * The default response reader that will just convert the body into a byte stream
 */
class DefaultResponseReader implements ResponseReader {

    @Override
    public InputStream read(Response response) {
        return ofNullable(response.body()).map(ResponseBody::byteStream).orElse(null);
    }
}
