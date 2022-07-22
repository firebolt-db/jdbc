package com.firebolt.jdbc.resultset.compress;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LZ4InputStreamTest {

  private final static String EXPECTED_TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
          + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
          + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi \n\n\n"
          + "ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "
          + "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
          + "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui "
          + "officia deserunt mollit anim id est laborum.";

  @Test
  void shouldReadCompressedText() throws IOException {

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    LZ4OutputStream outputStream = new LZ4OutputStream(byteArrayOutputStream, 1);
    outputStream.write(EXPECTED_TEXT.getBytes());
    outputStream.flush();
    byte[] result = byteArrayOutputStream.toByteArray();
    LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream(result));
    String decompressedText =
        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    assertEquals(EXPECTED_TEXT, decompressedText);
  }

  @Test
  void shouldReadyAllTextByteByByte() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    LZ4OutputStream outputStream = new LZ4OutputStream(byteArrayOutputStream, 1);
    outputStream.write(EXPECTED_TEXT.getBytes());
    outputStream.flush();
    byte[] result = byteArrayOutputStream.toByteArray();
    LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream(result));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int read = is.read();
    while (read > 0) {
      out.write(read);
      read = is.read();
    }
    out.flush();
    assertEquals(EXPECTED_TEXT, out.toString());
  }

  @Test
  void shouldThrowExceptionWhenByteArrayIsNull() {
    LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
    assertThrows(NullPointerException.class, () ->  is.read(null, 5,5));
  }

  @Test
  void shouldThrowExceptionWhenLength() {
    LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
    assertThrows(IndexOutOfBoundsException.class, () ->  is.read(new byte[0], -1,5));
  }

  @Test
  void shouldReturnZeroWhenLengthIs0() throws IOException {
    LZ4InputStream is = new LZ4InputStream(new ByteArrayInputStream("".getBytes()));
    assertEquals(0, is.read(new byte[1], 1,0));
  }
}
