package com.firebolt.jdbc.client.query.response.s3;

import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

final class ParallelS3FileDownloader implements S3FileDownloader {

	@Override
	public InputStream openCombinedInputStream(List<FireboltS3Response.S3DataChunks> chunks, AwsInfo awsInfo) {
		if (chunks == null || chunks.isEmpty()) {
			return null;
		}

		ExecutorService executor = Executors.newFixedThreadPool(determinePoolSize(chunks.size()));
		List<Future<Path>> futureFiles = new ArrayList<>(chunks.size());
		for (FireboltS3Response.S3DataChunks chunk : chunks) {
			futureFiles.add(executor.submit(downloadToTempFile(chunk.getUrl(), awsInfo)));
		}
		executor.shutdown();

		Iterator<Future<Path>> futureIterator = futureFiles.iterator();
		AtomicInteger chunkIndex = new AtomicInteger(0);
		Iterator<FireboltS3Response.S3DataChunks> chunkIter = chunks.iterator();
		Enumeration<InputStream> streamEnumeration = new Enumeration<>() {
			@Override
			public boolean hasMoreElements() {
				return futureIterator.hasNext();
			}

			@Override
			public InputStream nextElement() {
				FireboltS3Response.S3DataChunks chunk = chunkIter.next();
				Future<Path> future = futureIterator.next();
				Path filePath;
				int currentIndex = chunkIndex.getAndIncrement();
				try {
					filePath = future.get();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException("Interrupted while waiting for S3 chunk download", e);
				} catch (ExecutionException e) {
					throw new RuntimeException("Failed to download S3 chunk", e.getCause());
				}
				File file = filePath.toFile();
				try {
					InputStream in = new FileInputStream(file);
					// Decompress if compressed (including first chunk)
					if (chunk.isCompressed()) {
						in = new LZ4InputStream(in);
					}
					// For all chunks except the first (index 0), skip the first two header lines
					if (currentIndex > 0) {
						in = new HeaderSkippingInputStream(in, 2);
					}
					return new DeletingFileInputStream(in, file.toPath());
				} catch (IOException e) {
					try {
						Files.deleteIfExists(file.toPath());
					} catch (IOException ignore) { }
					throw new RuntimeException("Failed to open downloaded S3 chunk for reading", e);
				}
			}
		};

		return new SequenceInputStream(streamEnumeration);
	}

	private int determinePoolSize(int numTasks) {
		int cores = Runtime.getRuntime().availableProcessors();
		int recommended = Math.min(Math.max(4, cores * 2), 16);
		return Math.min(recommended, Math.max(1, numTasks));
	}

	private Callable<Path> downloadToTempFile(String s3Url, AwsInfo awsInfo) {
		return () -> {
			Path tempFile = Files.createTempFile("firebolt-s3-chunk-", ".bin");
			try (InputStream in = FireboltS3Client.getInstance(awsInfo).readObject(s3Url)) {
				Files.copy(in, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				return tempFile;
			} catch (IOException e) {
				try {
					Files.deleteIfExists(tempFile);
				} catch (IOException ignore) { }
				throw e;
			}
		};
	}

	private static final class DeletingFileInputStream extends FilterInputStream {
		private final Path filePath;
		DeletingFileInputStream(InputStream delegate, Path filePath) {
			super(delegate);
			this.filePath = filePath;
		}
		@Override
		public void close() throws IOException {
			IOException first = null;
			try {
				super.close();
			} catch (IOException e) {
				first = e;
			}
			try {
				Files.deleteIfExists(filePath);
			} catch (IOException e) {
				if (first == null) {
					first = e;
				}
			}
			if (first != null) {
				throw first;
			}
		}
	}

}


