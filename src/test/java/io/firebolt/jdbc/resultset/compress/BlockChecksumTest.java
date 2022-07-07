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
 *  - https://github.com/ClickHouse/clickhouse-jdbc/blob/ea5aaf579b0612bcf1825eb1ec31bf9b170a7a65/clickhouse-jdbc/src/test/java/ru/yandex/clickhouse/util/ClickHouseBlockChecksumTest.java
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Testing library from Testng to Jupiter
 *  - Testing library arguments order
 *  - Package name
 *  - Formatting
 *
 */
package io.firebolt.jdbc.resultset.compress;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Anton Sukhonosenko <a href="mailto:algebraic@yandex-team.ru"></a>
 * @date 08.06.18
 */
class BlockChecksumTest {
  private static final int HEADER_SIZE_BYTES = 9;

  private static int hexToBin(char ch) {
    if ('0' <= ch && ch <= '9') {
      return ch - '0';
    }
    if ('A' <= ch && ch <= 'F') {
      return ch - 'A' + 10;
    }
    if ('a' <= ch && ch <= 'f') {
      return ch - 'a' + 10;
    }
    return -1;
  }

  private static byte[] parseHexBinary(String s) {
    final int len = s.length();

    // "111" is not a valid hex encoding.
    if (len % 2 != 0) {
      throw new IllegalArgumentException("hexBinary needs to be even-length: " + s);
    }

    byte[] out = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      int h = hexToBin(s.charAt(i));
      int l = hexToBin(s.charAt(i + 1));
      if (h == -1 || l == -1) {
        throw new IllegalArgumentException("contains illegal character for hexBinary: " + s);
      }

      out[i / 2] = (byte) (h * 16 + l);
    }

    return out;
  }

  @Test
  void trickyBlock() {
    byte[] compressedData = parseHexBinary("1F000100078078000000B4000000");
    int uncompressedSizeBytes = 35;

    BlockChecksum checksum =
        BlockChecksum.calculateForBlock(
            (byte) LZ4InputStream.MAGIC,
            compressedData.length + HEADER_SIZE_BYTES,
            uncompressedSizeBytes,
            compressedData,
            compressedData.length);

    assertEquals(new BlockChecksum(-493639813825217902L, -6253550521065361778L), checksum);
  }

  @Test
  void anotherTrickyBlock() {
    byte[] compressedData = parseHexBinary("80D9CEF753E3A59B3F");
    int uncompressedSizeBytes = 8;

    BlockChecksum checksum =
        BlockChecksum.calculateForBlock(
            (byte) LZ4InputStream.MAGIC,
            compressedData.length + HEADER_SIZE_BYTES,
            uncompressedSizeBytes,
            compressedData,
            compressedData.length);

    assertEquals(new BlockChecksum(-7135037831041210418L, -8214889029657590490L), checksum);
  }
}
