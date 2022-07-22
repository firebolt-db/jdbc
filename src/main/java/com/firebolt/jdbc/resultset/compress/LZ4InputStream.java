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
 *  URLs :
 *  - Main class: https://github.com/ClickHouse/clickhouse-jdbc/blob/ea5aaf579b0612bcf1825eb1ec31bf9b170a7a65/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/response/ClickHouseLZ4Stream.java
 *  - Utils class from which readInt and readFully method was taken: https://github.com/ClickHouse/clickhouse-jdbc/blob/094ed0b9d2dd8a18ae0c7b3f8f22c35e595822a6/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/util/Utils.java
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Package name
 *  - Formatting
 *  - Import several readInt and readFully methods from ru.yandex.clickhouse.util.Utils class
 */
package com.firebolt.jdbc.resultset.compress;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Reader from clickhouse in lz4 */
public class LZ4InputStream extends InputStream {

  private static final LZ4Factory factory = LZ4Factory.fastestInstance();

  public static final int MAGIC = 0x82;

  private final InputStream stream;
  private final DataInputStream dataWrapper;

  private byte[] currentBlock;
  private int pointer;

  public LZ4InputStream(InputStream stream) {
    this.stream = stream;
    dataWrapper = new DataInputStream(stream);
  }

  @Override
  public int read() throws IOException {
    if (!checkNext()) return -1;
    byte b = currentBlock[pointer];
    pointer += 1;
    return b & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    if (!checkNext()) return -1;

    int copied = 0;
    int targetPointer = off;
    while (copied != len) {
      int toCopy = Math.min(currentBlock.length - pointer, len - copied);
      System.arraycopy(currentBlock, pointer, b, targetPointer, toCopy);
      targetPointer += toCopy;
      pointer += toCopy;
      copied += toCopy;
      if (!checkNext()) { // finished
        return copied;
      }
    }
    return copied;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  private boolean checkNext() throws IOException {
    if (currentBlock == null || pointer == currentBlock.length) {
      currentBlock = readNextBlock();
      pointer = 0;
    }
    return currentBlock != null;
  }

  // every block is:
  public byte[] readNextBlock() throws IOException {
    int read = stream.read();
    if (read < 0) return null;

    byte[] checksum = new byte[16];
    checksum[0] = (byte) read;
    // checksum - 16 bytes.
    readFully(dataWrapper, checksum, 1, 15);
    BlockChecksum expected = BlockChecksum.fromBytes(checksum);
    // header:
    // 1 byte - 0x82 (shows this is LZ4)
    int magic = dataWrapper.readUnsignedByte();
    if (magic != MAGIC) throw new IOException("Magic is not correct: " + magic);
    // 4 bytes - size of the compressed data including 9 bytes of the header
    int compressedSizeWithHeader = readInt(dataWrapper);
    // 4 bytes - size of uncompressed data
    int uncompressedSize = readInt(dataWrapper);
    int compressedSize = compressedSizeWithHeader - 9; // header
    byte[] block = new byte[compressedSize];
    // compressed data: compressed_size - 9 байт.
    readFully(dataWrapper, block);

    BlockChecksum real =
        BlockChecksum.calculateForBlock(
            (byte) magic, compressedSizeWithHeader, uncompressedSize, block, compressedSize);
    if (!real.equals(expected)) {
      throw new IllegalArgumentException("Checksum doesn't match: corrupted data.");
    }

    byte[] decompressed = new byte[uncompressedSize];
    LZ4FastDecompressor decompressor = factory.fastDecompressor();
    decompressor.decompress(block, 0, decompressed, 0, uncompressedSize);
    return decompressed;
  }

  // This method was picked from ru.yandex.clickhouse.util.Utils
  private static void readFully(DataInputStream in, byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(in);
    Objects.requireNonNull(b);
    if (len < 0 || off < 0) {
      throw new IndexOutOfBoundsException(
          String.format("length (%s) and offset (%s) cannot be negative", len, off));
    }
    int end = off + len;
    if (end < off || end > b.length) {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset (%s) should less than length (%s) and buffer length (%s)",
              off, len, b.length));
    }

    int total = 0;
    while (total < len) {
      int result = in.read(b, off + total, len - total);
      if (result == -1) {
        break;
      }
      total += result;
    }

    if (total != len) {
      throw new EOFException(
          "reached end of stream after reading " + total + " bytes; " + len + " bytes expected");
    }
  }

  // This method was picked from ru.yandex.clickhouse.util.Utils
  private static int readInt(DataInputStream inputStream) throws IOException {
    byte b1 = (byte) inputStream.readUnsignedByte();
    byte b2 = (byte) inputStream.readUnsignedByte();
    byte b3 = (byte) inputStream.readUnsignedByte();
    byte b4 = (byte) inputStream.readUnsignedByte();

    return b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
  }

  // This method was picked from ru.yandex.clickhouse.util.Utils
  private static void readFully(DataInputStream in, byte[] b) throws IOException {
    readFully(in, b, 0, b.length);
  }
}
