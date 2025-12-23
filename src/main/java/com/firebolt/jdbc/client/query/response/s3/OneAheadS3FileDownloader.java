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
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Downloader that prefetches exactly one chunk ahead.
 * While chunk N is being read, chunk N+1 is downloaded in the background to a temp file.
 * Subsequent chunks (index > 0) will have the first two header lines skipped.
 */
final class OneAheadS3FileDownloader implements S3FileDownloader {

	@Override
	public InputStream openCombinedInputStream(List<FireboltS3Response.S3DataChunks> chunks, AwsInfo awsInfo) {
		if (chunks == null || chunks.isEmpty()) {
            return null;
        }

		final int total = chunks.size();
		final ExecutorService executor = Executors.newSingleThreadExecutor();

		Enumeration<InputStream> enumeration = new Enumeration<>() {
			private int index = 0;
			private Future<Path> nextFuture = null; // future for chunk at (index)

			@Override
			public boolean hasMoreElements() {
				return index < total;
			}

			@Override
			public InputStream nextElement() {
				int current = index++;
				try {
					// If current is 0, we haven't prefetched anything yet.
					// Schedule prefetch of chunk 1 (if exists), then return network stream for chunk 0.
					if (current == 0) {
						if (total > 1) {
							nextFuture = executor.submit(downloadToTempFile(chunks.get(1).getUrl(), awsInfo));
						} else {
							// No prefetch needed; we can shut down immediately as there will be no tasks.
							executor.shutdown();
						}
						InputStream in = FireboltS3Client.getInstance(awsInfo).readObject(chunks.get(0).getUrl());
						if (chunks.get(0).isCompressed()) {
							in = new LZ4InputStream(in);
						}
						return in;
					}

					// For current > 0, the future should correspond to the current chunk file.
					if (nextFuture == null) {
						// This should not happen unless there was only one chunk total
						// In that case, return a direct stream (defensive)
						FireboltS3Response.S3DataChunks thisChunk = chunks.get(current);
						InputStream fallback = FireboltS3Client.getInstance(awsInfo).readObject(thisChunk.getUrl());
						if (thisChunk.isCompressed()) {
							fallback = new LZ4InputStream(fallback);
						}
						InputStream in = new HeaderSkippingInputStream(fallback, 2);
						// Shut down if we reached the end
						if (current == total - 1) {
							executor.shutdown();
						} else {
							nextFuture = executor.submit(downloadToTempFile(chunks.get(current + 1).getUrl(), awsInfo));
						}
						return in;
					}

					Path filePath = nextFuture.get();
					File file = filePath.toFile();

					// Schedule prefetch for the following chunk before returning the current one
					if (current < total - 1) {
						nextFuture = executor.submit(downloadToTempFile(chunks.get(current + 1).getUrl(), awsInfo));
					} else {
						// No further prefetch; we can shut down the executor
						executor.shutdown();
						nextFuture = null;
					}

					InputStream in = new FileInputStream(file);
					if (chunks.get(current).isCompressed()) {
						in = new LZ4InputStream(in);
					}
					// Skip headers for all chunks except the first
					in = new HeaderSkippingInputStream(in, 2);
					return new DeletingFileInputStream(in, file.toPath());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException("Interrupted while waiting for S3 chunk prefetch", e);
				} catch (ExecutionException e) {
					throw new RuntimeException("Failed to prefetch S3 chunk", e.getCause());
				} catch (IOException e) {
					throw new RuntimeException("Failed to open prefetched S3 chunk", e);
				}
			}
		};

		return new SequenceInputStream(enumeration);
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


