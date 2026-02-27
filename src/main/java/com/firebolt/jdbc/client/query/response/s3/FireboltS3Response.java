package com.firebolt.jdbc.client.query.response.s3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Comparator;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * This would be the response from firebolt that represents the information of the s3 bucket(s) where the actual results are stored
 */
@Getter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class FireboltS3Response {

    @JsonProperty("total_bytes")
    private long totalBytes;

    @JsonProperty("total_files")
    private int totalFiles;

    @JsonProperty("total_rows")
    private long totalRows;

    @JsonProperty("chunks")
    private List<S3DataChunks> chunks;

    /**
     * The information about a particular data file in s3
     */
    @Getter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class S3DataChunks {

        public static final Comparator<S3DataChunks> CHUNK_ID_COMPARATOR = Comparator.comparing(s3DataChunks -> s3DataChunks.getChunkId());

        @JsonProperty("bytes")
        private long bytes;

        @JsonProperty("chunk_id")
        private int chunkId;

        @JsonProperty("compressed")
        private boolean compressed;

        @JsonProperty("rows")
        private long rows;

        @JsonProperty("url")
        private String url;

    }

}
