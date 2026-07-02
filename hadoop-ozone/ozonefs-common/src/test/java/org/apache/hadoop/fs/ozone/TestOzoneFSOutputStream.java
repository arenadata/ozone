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

package org.apache.hadoop.fs.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.junit.jupiter.api.Test;

/**
 * Tests the direct {@link ByteBuffer} write path of {@link OzoneFSOutputStream}
 * and the {@code WRITEBYTEBUFFER} capability of
 * {@link CapableOzoneFSOutputStream}.
 */
public class TestOzoneFSOutputStream {

  private static CapableOzoneFSOutputStream capableOver(OzoneOutputStream oos) {
    return new CapableOzoneFSOutputStream(new OzoneFSOutputStream(oos), true);
  }

  @Test
  public void advertisesWriteByteBufferForReplicatedKey() {
    KeyOutputStream kos = mock(KeyOutputStream.class);
    CapableOzoneFSOutputStream out = capableOver(new OzoneOutputStream(kos, false));

    assertTrue(out.hasCapability(StreamCapabilities.WRITEBYTEBUFFER));
  }

  @Test
  public void doesNotAdvertiseWriteByteBufferForErasureCodedKey() {
    ECKeyOutputStream kos = mock(ECKeyOutputStream.class);
    CapableOzoneFSOutputStream out = capableOver(new OzoneOutputStream(kos, false));

    assertFalse(out.hasCapability(StreamCapabilities.WRITEBYTEBUFFER));
  }

  @Test
  public void doesNotAdvertiseWriteByteBufferWhenEncrypted() {
    KeyOutputStream kos = mock(KeyOutputStream.class);
    CryptoOutputStream cos = mock(CryptoOutputStream.class);
    when(cos.getWrappedStream()).thenReturn(kos);
    CapableOzoneFSOutputStream out = capableOver(new OzoneOutputStream(cos, null));

    assertFalse(out.hasCapability(StreamCapabilities.WRITEBYTEBUFFER));
    // Encryption still advertises the flush capabilities of the wrapped key.
    assertTrue(out.hasCapability(StreamCapabilities.HFLUSH));
  }

  @Test
  public void writeByteBufferForwardsToKeyStreamAndConsumesBuffer()
      throws IOException {
    KeyOutputStream kos = mock(KeyOutputStream.class);
    OzoneFSOutputStream out = new OzoneFSOutputStream(new OzoneOutputStream(kos, false));

    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.position(4).limit(12);
    out.write(buf);

    // Whole remaining region forwarded at its absolute offset, buffer drained.
    verify(kos).write(same(buf), eq(4), eq(8));
    assertEquals(12, buf.position(), "position must advance to limit");
    assertFalse(buf.hasRemaining());
  }

  @Test
  public void writeByteBufferFallsBackToHeapWhenEncrypted() throws IOException {
    CryptoOutputStream cos = mock(CryptoOutputStream.class);
    OzoneFSOutputStream out = new OzoneFSOutputStream(new OzoneOutputStream(cos, null));

    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.position(0).limit(8);
    out.write(buf);

    // CryptoOutputStream is byte[]-only, so the fallback drains via write(byte[]).
    verify(cos).write(same(buf.array()), eq(0), eq(8));
    assertEquals(8, buf.position(), "position must advance to limit");
  }
}
