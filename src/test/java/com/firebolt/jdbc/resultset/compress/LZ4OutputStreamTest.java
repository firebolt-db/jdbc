/*
 * Copyright 2022 Firebolt Analytics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY Firebolt Analytics, Inc. UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
OF THE COMPANY YANDEX LLC.
 * URL:
 *  - https://github.com/ClickHouse/clickhouse-jdbc/blob/ea5aaf579b0612bcf1825eb1ec31bf9b170a7a65/clickhouse-jdbc/src/test/java/ru/yandex/clickhouse/util/ClickHouseLZ4OutputStreamTest.java
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Testing library from Testng to Jupiter
 *  - Testing library arguments order
 *  - Package name
 *  - Formatting
 *  - Method names
 *  - Method positions
 */
package com.firebolt.jdbc.resultset.compress;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LZ4OutputStreamTest {
  @Test
  void testWrite() throws IOException {
    ByteArrayOutputStream bas = new ByteArrayOutputStream(64);

    try (LZ4OutputStream out = new LZ4OutputStream(bas, 2)) {
      byte[] bytes =
          new byte[] {
            (byte) -36,
            (byte) -86,
            (byte) 31,
            (byte) 113,
            (byte) -106,
            (byte) 44,
            (byte) 99,
            (byte) 96,
            (byte) 112,
            (byte) -7,
            (byte) 47,
            (byte) 15,
            (byte) -63,
            (byte) 39,
            (byte) -73,
            (byte) -104,
            (byte) -126,
            (byte) 12,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 2,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 32,
            (byte) 1,
            (byte) 2
          };
      out.write(1);
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.write(2);
      assertArrayEquals(bytes, bas.toByteArray());
      out.write(3);
      assertArrayEquals(bytes, bas.toByteArray());
      out.flush();
      assertArrayEquals(
          new byte[] {
            (byte) -36,
            (byte) -86,
            (byte) 31,
            (byte) 113,
            (byte) -106,
            (byte) 44,
            (byte) 99,
            (byte) 96,
            (byte) 112,
            (byte) -7,
            (byte) 47,
            (byte) 15,
            (byte) -63,
            (byte) 39,
            (byte) -73,
            (byte) -104,
            (byte) -126,
            (byte) 12,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 2,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 32,
            (byte) 1,
            (byte) 2,
            (byte) 64,
            (byte) -39,
            (byte) 21,
            (byte) 50,
            (byte) -77,
            (byte) -124,
            (byte) 25,
            (byte) 73,
            (byte) -59,
            (byte) 9,
            (byte) 112,
            (byte) -38,
            (byte) 12,
            (byte) 99,
            (byte) 71,
            (byte) 74,
            (byte) -126,
            (byte) 11,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 1,
            (byte) 0,
            (byte) 0,
            (byte) 0,
            (byte) 16,
            (byte) 3
          },
          bas.toByteArray());
      bas.close();
    }
  }

  @Test
  void testWriteBytes() throws IOException {
    assertThrows(
        NullPointerException.class,
        () -> {
          try (LZ4OutputStream out = new LZ4OutputStream(new ByteArrayOutputStream(), 3)) {
            out.write(null);
          }
        });

    ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(new byte[0]);
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.flush();
      assertArrayEquals(new byte[0], bas.toByteArray());

      byte[] bytes = new byte[] {(byte) 13, (byte) 13};
      out.write(bytes);
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.flush();
      assertArrayEquals(genCompressedBytes(13, 2, 3), bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      byte[] bytes = new byte[] {(byte) 13, (byte) 13, (byte) 13};
      out.write(bytes);
      byte[] expected = genCompressedBytes(13, 3, 3);
      assertArrayEquals(expected, bas.toByteArray());
      out.flush();
      assertArrayEquals(expected, bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      byte[] bytes = new byte[] {(byte) 13, (byte) 13, (byte) 13, (byte) 13};
      out.write(bytes);
      assertArrayEquals(genCompressedBytes(13, 3, 3), bas.toByteArray());
      out.flush();
      assertArrayEquals(genCompressedBytes(13, 4, 3), bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      byte[] bytes = new byte[] {(byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13};
      out.write(bytes);
      byte[] expected = genCompressedBytes(13, 6, 3);
      assertArrayEquals(expected, bas.toByteArray());
      out.flush();
      assertArrayEquals(expected, bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      byte[] bytes =
          new byte[] {(byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13};
      out.write(bytes);
      assertArrayEquals(genCompressedBytes(13, 6, 3), bas.toByteArray());
      out.flush();
      assertArrayEquals(genCompressedBytes(13, 7, 3), bas.toByteArray());
      bas.close();
    }
  }

  @Test
  void testWriteBytesWithOffset() throws IOException {
    assertThrows(
        NullPointerException.class,
        () -> {
          try (LZ4OutputStream out = new LZ4OutputStream(new ByteArrayOutputStream(), 3)) {
            out.write(null, 0, 1);
          }
        });

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          try (LZ4OutputStream out = new LZ4OutputStream(new ByteArrayOutputStream(), 3)) {
            out.write(new byte[0], 0, 1);
          }
        });

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          try (LZ4OutputStream out = new LZ4OutputStream(new ByteArrayOutputStream(), 3)) {
            out.write(new byte[0], -1, 0);
          }
        });

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          try (LZ4OutputStream out = new LZ4OutputStream(new ByteArrayOutputStream(), 3)) {
            out.write(new byte[1], 1, 1);
          }
        });

    final byte[] bytes =
        new byte[] {
          (byte) 0, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13,
          (byte) 0
        };
    ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(bytes, 1, 0);
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.flush();
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.write(bytes, 1, 2);
      assertArrayEquals(new byte[0], bas.toByteArray());
      out.flush();
      assertArrayEquals(bas.toByteArray(), genCompressedBytes(13, 2, 3));
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(bytes, 1, 3);
      byte[] expected = genCompressedBytes(13, 3, 3);
      assertArrayEquals(expected, bas.toByteArray());
      out.flush();
      assertArrayEquals(expected, bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(bytes, 1, 4);
      assertArrayEquals(genCompressedBytes(13, 3, 3), bas.toByteArray());
      out.flush();
      assertArrayEquals(genCompressedBytes(13, 4, 3), bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(bytes, 1, 6);
      byte[] expected = genCompressedBytes(13, 6, 3);
      assertArrayEquals(expected, bas.toByteArray());
      out.flush();
      assertArrayEquals(expected, bas.toByteArray());
      bas.close();
    }

    bas = new ByteArrayOutputStream(64);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, 3)) {
      out.write(bytes, 1, 7);
      assertArrayEquals(genCompressedBytes(13, 6, 3), bas.toByteArray());
      out.flush();
      assertArrayEquals(genCompressedBytes(13, 7, 3), bas.toByteArray());
      bas.close();
    }
  }

  private byte[] genCompressedBytes(int b, int length, int blockSize) throws IOException {
    ByteArrayOutputStream bas = new ByteArrayOutputStream(blockSize * 512);
    try (LZ4OutputStream out = new LZ4OutputStream(bas, blockSize)) {
      for (int i = 0; i < length; i++) {
        out.write(b);
      }
      out.flush();
    }

    byte[] bytes = bas.toByteArray();
    bas.close();
    return bytes;
  }
}
