package com.firebolt.jdbc.client.query.response.s3;

import java.util.Arrays;

public enum S3DownloaderType {
    /**
     * Download files from s3 one chunk after another as they get read by the input stream
     */
    SEQUENTIAL("sequential"),

    /**
     * Download all the chunks to local temp files in parallel and then stream them from local disk when asked for it
     */
    PARALLEL("parallel"),

    /**
     * Download the current chunk and one ahead
     */
    ONE_AHEAD("one_ahead");

    private String type;

    S3DownloaderType(String type) {
        this.type = type;
    }

    public static final S3DownloaderType fromValue(String value) {
        return Arrays.stream(S3DownloaderType.values())
                .filter(val -> val.type.equals(value.toLowerCase()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find s3 downloader type for value " + value));
    }
}
