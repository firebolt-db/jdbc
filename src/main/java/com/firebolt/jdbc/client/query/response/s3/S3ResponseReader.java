package com.firebolt.jdbc.client.query.response.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.query.response.ResponseReader;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okhttp3.ResponseBody;

import static java.util.Optional.ofNullable;

/**
 * Read the response from s3 location. The initial response will contain the location of the s3 bucket from where to read the actual query response
 */
@Slf4j
public class S3ResponseReader implements ResponseReader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private AwsInfo awsInfo;
    private boolean responseCompressed;
    private S3DownloaderType type;
    private final S3FileDownloaderProvider s3FileDownloaderProvider;

    public S3ResponseReader(AwsInfo awsInfo, boolean responseCompressed, S3DownloaderType type) {
        this(awsInfo, responseCompressed, type, new S3FileDownloaderProvider());
	}

//	@VisibleForTesting
	S3ResponseReader(AwsInfo awsInfo, boolean responseCompressed, S3DownloaderType type, S3FileDownloaderProvider s3FileDownloaderProvider) {
			this.awsInfo = awsInfo;
			this.responseCompressed = responseCompressed;
            this.type = type;
			this.s3FileDownloaderProvider = s3FileDownloaderProvider;
    }

    /**
     * Reads the response from firebolt. In case compression is used, the data will be decompressed using lz4 algo.
     *
     * It then parses the result to find out the location of the actual result data and make calls to get the results from that location
     * @param response
     * @return
     */
    @Override
    public InputStream read(Response response) {
        Optional<FireboltS3Response> fireboltS3ResponseOptional =
                ofNullable(response.body())
                        .map(ResponseBody::byteStream)
                        .map(bodyStream -> decompressResponseIfNeeded(bodyStream))
                        .map(bodyStream -> {
                            try {
                                return OBJECT_MAPPER.readValue(bodyStream, FireboltS3Response.class);
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to parse Firebolt S3 response", e);
                            }
                        });

        if (fireboltS3ResponseOptional.isEmpty()) {
            log.warn("Empty response received when it should receive the location for the s3 file");
            return null;
        }

        FireboltS3Response fireboltS3Response = fireboltS3ResponseOptional.get();

        if (fireboltS3Response.getChunks() == null || fireboltS3Response.getChunks().isEmpty()) {
            log.warn("S3 response did not contain any data files");
            return null;
        }

        // order by chunk_id and extract valid chunks (non-null url)
        List<FireboltS3Response.S3DataChunks> chunks = fireboltS3Response.getChunks().stream()
                .sorted(FireboltS3Response.S3DataChunks.CHUNK_ID_COMPARATOR)
                .filter(c -> c.getUrl() != null && !c.getUrl().isEmpty())
                .collect(Collectors.toList());

        if (chunks.isEmpty()) {
            log.warn("S3 response did not contain any valid data file paths");
            return null;
        }

		return s3FileDownloaderProvider.get(type).openCombinedInputStream(chunks, awsInfo);
    }

    private InputStream decompressResponseIfNeeded(InputStream inputStream) {
        if (!responseCompressed) {
            return inputStream;
        }

        log.debug("Decompressing the response using lz4 algorithm");
        return new LZ4InputStream(inputStream);
    }
}
