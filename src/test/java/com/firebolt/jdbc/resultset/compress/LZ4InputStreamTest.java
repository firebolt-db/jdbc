package com.firebolt.jdbc.resultset.compress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class LZ4InputStreamTest {

	private final static String EXPECTED_TEXT = "my_text\n" + "text\n"
			+ "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry\\'s standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.";

	@Test
	void shouldReadCompressedText() throws IOException {
		byte[] b = convertInputStreamToBytes(getInputStreamWithDates());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(b);
		baos.flush();
		LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream(baos.toByteArray()));
		String decompressedText = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
				.collect(Collectors.joining("\n"));
		assertEquals(EXPECTED_TEXT, decompressedText);
	}

	@Test
	void shouldThrowExceptionWhenByteArrayIsNull() {
		LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
		assertThrows(NullPointerException.class, () -> is.read(null, 5, 5));
	}

	@Test
	void shouldThrowExceptionWhenLength() {
		LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
		assertThrows(IndexOutOfBoundsException.class, () -> is.read(new byte[0], -1, 5));
	}

	@Test
	void shouldReturnZeroWhenLengthIs0() throws IOException {
		LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
		assertEquals(0, is.read(new byte[1], 1, 0));
	}

	private static byte[] convertInputStreamToBytes(InputStream stream) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] buffer = new byte[4096];
		int bytesRead;
		while ((bytesRead = stream.read(buffer)) != -1) {
			outputStream.write(buffer, 0, bytesRead);
		}
		return outputStream.toByteArray();
	}

	private InputStream getInputStreamWithDates() {
		return LZ4InputStreamTest.class.getResourceAsStream("/responses/compressed-response");
	}
}
