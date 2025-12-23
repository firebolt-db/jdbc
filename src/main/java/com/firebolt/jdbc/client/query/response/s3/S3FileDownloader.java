package com.firebolt.jdbc.client.query.response.s3;

import java.io.InputStream;
import java.util.List;

/**
 * Abstraction that downloads a list of S3 object paths and exposes them as a single InputStream
 * that yields bytes in the same order as the provided paths.
 */
public interface S3FileDownloader {

	InputStream openCombinedInputStream(List<FireboltS3Response.S3DataChunks> chunks, AwsInfo awsInfo);

}


