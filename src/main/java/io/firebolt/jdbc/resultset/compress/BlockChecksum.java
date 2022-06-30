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
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Package name
 *  - Formatting
 *  - Remove unnecessary type casts
 *
 */
package io.firebolt.jdbc.resultset.compress;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockChecksum {
  private final long first;
  private final long second;

  public BlockChecksum(long first, long second) {
    this.first = first;
    this.second = second;
  }

  public static BlockChecksum fromBytes(byte[] checksum) {
    ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).put(checksum);
    ((Buffer) buffer).flip();
    return new BlockChecksum(buffer.getLong(), buffer.getLong());
  }

  public static BlockChecksum calculateForBlock(
      byte magic, int compressedSizeWithHeader, int uncompressedSize, byte[] data, int length) {
    ByteBuffer buffer =
        ByteBuffer.allocate(compressedSizeWithHeader)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(magic)
            .putInt(compressedSizeWithHeader)
            .putInt(uncompressedSize)
            .put(data, 0, length);
    buffer.flip();
    return calculate(buffer.array());
  }

  public byte[] asBytes() {
    ByteBuffer buffer =
        ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(first).putLong(second);
    ((Buffer) buffer).flip();
    return buffer.array();
  }

  private static BlockChecksum calculate(byte[] data) {
    long[] sum = CityHash.cityHash128(data, 0, data.length);
    return new BlockChecksum(sum[0], sum[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BlockChecksum that = (BlockChecksum) o;

    if (first != that.first) return false;
    return second == that.second;
  }

  @Override
  public int hashCode() {
    int result = (int) (first ^ (first >>> 32));
    result = 31 * result + (int) (second ^ (second >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "{" + first + ", " + second + '}';
  }
}
