package com.firebolt.jdbc.client.query.response.s3;

import java.io.InputStream;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Thin wrapper around AWS SDK v2 S3 client to fetch objects given an S3 path.
 * Supports paths in the form s3://bucket/key.
 */
@Slf4j
public final class FireboltS3Client implements AutoCloseable {

	private static final Pattern S3_URI = Pattern.compile("^s3://([^/]+)/(.+)$");
	private static volatile FireboltS3Client INSTANCE;

	private final S3Client s3;

	private FireboltS3Client(AwsInfo awsInfo) {
		Region region = StringUtils.isEmpty(awsInfo.getRegion()) ? new DefaultAwsRegionProviderChain().getRegion() : Region.of(awsInfo.getRegion());
		log.info("Using AWS region {}", region);

		S3ClientBuilder builder = S3Client.builder()
				.credentialsProvider(getCredentialsProvider(awsInfo));

		if (region != null) {
			builder = builder.region(region);
		}

		this.s3 = builder.build();
	}

	private AwsCredentialsProvider getCredentialsProvider(AwsInfo awsInfo) {
		if (StringUtils.isAnyEmpty(awsInfo.getKeyId(), awsInfo.getKeySecret())) {
			return DefaultCredentialsProvider.create();
		}

		AwsCredentials awsCredentials = StringUtils.isEmpty(awsInfo.getSessionToken())
				? AwsBasicCredentials.create(awsInfo.getKeyId(), awsInfo.getKeySecret()) : AwsSessionCredentials.create(awsInfo.getKeyId(), awsInfo.getKeySecret(), awsInfo.getSessionToken());
		return StaticCredentialsProvider.create(awsCredentials);
	}

	/**
	 * Get a singleton instance. This avoids repeatedly creating SDK clients.
	 */
	public static FireboltS3Client getInstance(AwsInfo awsInfo) {
		if (INSTANCE == null) {
			synchronized (FireboltS3Client.class) {
				if (INSTANCE == null) {
					INSTANCE = new FireboltS3Client(awsInfo);
				}
			}
		}
		return INSTANCE;
	}

	/**
	 * Read an object from S3 and return its stream.
	 * The caller is responsible for closing the returned {@link InputStream}.
	 *
	 * @param s3Path path like s3://bucket/key
	 * @return input stream to the object contents
	 */
	public InputStream readObject(@NonNull String s3Path) {
		Objects.requireNonNull(s3Path, "s3Path must not be null");
		Matcher matcher = S3_URI.matcher(s3Path);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("Unsupported S3 path format: " + s3Path);
		}
		String bucket = matcher.group(1);
		String key = matcher.group(2);

		GetObjectRequest request = GetObjectRequest.builder()
				.bucket(bucket)
				.key(key)
				.build();

		log.debug("Fetching S3 object. bucket={}, key={}", bucket, key);
		ResponseInputStream<?> responseStream = s3.getObject(request);
		return responseStream;
	}

	@Override
	public void close() {
		try {
			this.s3.close();
		} catch (Exception e) {
			log.warn("Error closing S3 client", e);
		}
	}
}


