package com.firebolt.jdbc.client.query.response.s3;

import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

final class SequentialS3FileDownloader implements S3FileDownloader {

	@Override
	public InputStream openCombinedInputStream(List<FireboltS3Response.S3DataChunks> chunks, AwsInfo awsInfo) {
		if (chunks == null || chunks.isEmpty()) {
			return null;
		}

		Iterator<FireboltS3Response.S3DataChunks> it = chunks.iterator();
		Enumeration<InputStream> enumeration = new Enumeration<>() {
			private int index = 0;
			@Override
			public boolean hasMoreElements() {
				return it.hasNext();
			}
			@Override
			public InputStream nextElement() {
				FireboltS3Response.S3DataChunks chunk = it.next();
				InputStream in = FireboltS3Client.getInstance(awsInfo).readObject(chunk.getUrl());
				if (chunk.isCompressed()) {
					in = new LZ4InputStream(in);
				}

				// For chunks after the first then skip headers
				if (index > 0) {
					in = new HeaderSkippingInputStream(in, 2);

				}
				index++;
				return in;
			}
		};

		return new SequenceInputStream(enumeration);
	}

}


