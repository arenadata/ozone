/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ByteArrayStreamOutput#write(ByteBuffer, int, int)}, in
 * particular that a logical {@code off} is translated correctly for heap
 * buffers with a non-zero {@link ByteBuffer#arrayOffset()} (e.g. slices).
 */
public class TestByteArrayStreamOutput {

  private static final byte[] DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  /** Collects everything written through the byte[] sink. */
  private static final class Collector extends ByteArrayStreamOutput {
    private final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    @Override
    public void write(byte[] b, int off, int len) {
      sink.write(b, off, len);
    }

    @Override
    public void flush() {
    }

    @Override
    public void hflush() {
    }

    @Override
    public void hsync() {
    }

    byte[] written() {
      return sink.toByteArray();
    }
  }

  private static void writeRemaining(Collector c, ByteBuffer buf)
      throws java.io.IOException {
    c.write(buf, buf.position(), buf.remaining());
  }

  @Test
  public void writesSlicedHeapBufferFromCorrectOffset() throws Exception {
    ByteBuffer base = ByteBuffer.allocate(DATA.length);
    base.put(DATA);
    base.position(3);
    ByteBuffer slice = base.slice(); // arrayOffset() == 3, content = DATA[3..]
    assertEquals(3, slice.arrayOffset());

    Collector c = new Collector();
    writeRemaining(c, slice);

    assertArrayEquals(Arrays.copyOfRange(DATA, 3, DATA.length), c.written());
  }

  @Test
  public void writesHeapBufferFromLogicalPosition() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(DATA.clone());
    buf.position(4).limit(9);

    Collector c = new Collector();
    writeRemaining(c, buf);

    assertArrayEquals(Arrays.copyOfRange(DATA, 4, 9), c.written());
  }

  @Test
  public void writesDirectBuffer() throws Exception {
    ByteBuffer buf = ByteBuffer.allocateDirect(DATA.length);
    buf.put(DATA);
    buf.position(2).limit(8);

    Collector c = new Collector();
    writeRemaining(c, buf);

    assertArrayEquals(Arrays.copyOfRange(DATA, 2, 8), c.written());
  }
}
