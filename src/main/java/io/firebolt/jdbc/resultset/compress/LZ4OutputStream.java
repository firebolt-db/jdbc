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
 *  - Main class: https://github.com/ClickHouse/clickhouse-jdbc/blob/094ed0b9d2dd8a18ae0c7b3f8f22c35e595822a6/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/util/ClickHouseLZ4OutputStream.java
 *  - Utils class from which the writeInt method was taken: https://github.com/ClickHouse/clickhouse-jdbc/blob/094ed0b9d2dd8a18ae0c7b3f8f22c35e595822a6/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/util/Utils.java
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Package name
 *  - Formatting
 *  - Import writeInt from Utils class
 */
package io.firebolt.jdbc.resultset.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LZ4OutputStream extends OutputStream {
  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private final DataOutputStream dataWrapper;
  private final LZ4Compressor compressor;
  private final byte[] currentBlock;
  private final byte[] compressedBlock;

  private int pointer;

  public LZ4OutputStream(OutputStream stream, int maxCompressBlockSize) {
    dataWrapper = new DataOutputStream(stream);
    compressor = factory.fastCompressor();
    currentBlock = new byte[maxCompressBlockSize];
    compressedBlock = new byte[compressor.maxCompressedLength(maxCompressBlockSize)];
  }

  /**
   * @return Location of pointer in the byte buffer (bytes not yet flushed)
   */
  public int position() {
    return pointer;
  }

  @Override
  public void write(int b) throws IOException {
    currentBlock[pointer] = (byte) b;
    pointer++;

    if (pointer == currentBlock.length) {
      writeBlock();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    int blockSize = currentBlock.length;
    int rest = blockSize - pointer;
    while (len >= rest) {
      System.arraycopy(b, off, currentBlock, pointer, rest);
      pointer += rest;
      writeBlock();
      off += rest;
      len -= rest;
      rest = blockSize;
    }

    if (len > 0) {
      System.arraycopy(b, off, currentBlock, pointer, len);
      pointer += len;
    }
  }

  @Override
  public void flush() throws IOException {
    if (pointer != 0) {
      writeBlock();
    }
    dataWrapper.flush();
  }

  private void writeBlock() throws IOException {
    int compressed = compressor.compress(currentBlock, 0, pointer, compressedBlock, 0);
    BlockChecksum checksum = BlockChecksum.calculateForBlock((byte) LZ4InputStream.MAGIC,
            compressed + 9, pointer, compressedBlock, compressed);
    dataWrapper.write(checksum.asBytes());
    dataWrapper.writeByte(LZ4InputStream.MAGIC);
    writeInt(dataWrapper, compressed + 9); // compressed size with header
    writeInt(dataWrapper, pointer); // uncompressed size
    dataWrapper.write(compressedBlock, 0, compressed);
    pointer = 0;
  }

  // This method was picked from ru.yandex.clickhouse.util.Utils
  private static void writeInt(DataOutputStream outputStream, int value) throws IOException {
    outputStream.write(0xFF & value);
    outputStream.write(0xFF & (value >> 8));
    outputStream.write(0xFF & (value >> 16));
    outputStream.write(0xFF & (value >> 24));
  }
}
