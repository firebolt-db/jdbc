package com.firebolt.jdbc.client.query.response.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * InputStream that discards the first N newline-terminated lines from the underlying stream,
 * handling both LF and CRLF by detecting '\n' as the line terminator.
 * Skipping is performed lazily on first read and uses a PushbackInputStream to preserve
 * any bytes read beyond the final skipped newline.
 */
public final class HeaderSkippingInputStream extends InputStream {
	private static final int DEFAULT_PUSHBACK_BUFFER = 8192;
	private final PushbackInputStream in;
	private final int linesToSkip;
	private boolean skipped = false;

	public HeaderSkippingInputStream(InputStream delegate, int linesToSkip) {
		this(delegate, linesToSkip, DEFAULT_PUSHBACK_BUFFER);
	}

	public HeaderSkippingInputStream(InputStream delegate, int linesToSkip, int pushbackBufferSize) {
		this.in = new PushbackInputStream(delegate, pushbackBufferSize);
		this.linesToSkip = linesToSkip;
	}

	private void ensureSkipped() throws IOException {
		if (skipped) {
			return;
		}
		int remaining = linesToSkip;
		byte[] buf = new byte[DEFAULT_PUSHBACK_BUFFER];
		while (remaining > 0) {
			int n = in.read(buf);
			if (n == -1) {
				break;
			}
			int lastNewlinePos = -1;
			for (int i = 0; i < n && remaining > 0; i++) {
				if (buf[i] == (byte) '\n') {
					remaining--;
					lastNewlinePos = i;
				}
			}
			if (remaining <= 0) {
				int remainderStart = lastNewlinePos + 1;
				int remainderLen = n - remainderStart;
				if (remainderLen > 0) {
					in.unread(buf, remainderStart, remainderLen);
				}
				break;
			}
		}
		skipped = true;
	}

	@Override
	public int read() throws IOException {
		ensureSkipped();
		return in.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		ensureSkipped();
		return in.read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		ensureSkipped();
		return in.skip(n);
	}

	@Override
	public int available() throws IOException {
		ensureSkipped();
		return in.available();
	}

	@Override
	public void close() throws IOException {
		in.close();
	}
}


