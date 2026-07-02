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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.ByteBufferWritable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a replicated {@link KeyOutputStream} exposes the
 * {@link ByteBufferWritable} contract used by a wrapping
 * {@code CryptoOutputStream} to forward ciphertext without a heap copy, and
 * that {@link ECKeyOutputStream} opts out of it.
 */
public class TestKeyOutputStreamByteBuffer {

  @Test
  public void replicatedKeyIsByteBufferWritableAndAdvertisesCapability() {
    KeyOutputStream kos = spy(KeyOutputStream.class);
    assertInstanceOf(ByteBufferWritable.class, kos);
    assertTrue(kos.hasCapability(StreamCapabilities.WRITEBYTEBUFFER));
  }

  @Test
  public void erasureCodedKeyDoesNotAdvertiseCapability() {
    ECKeyOutputStream kos = mock(ECKeyOutputStream.class, CALLS_REAL_METHODS);
    assertFalse(kos.hasCapability(StreamCapabilities.WRITEBYTEBUFFER));
  }

  @Test
  public void writeByteBufferDelegatesAndConsumesBuffer() throws IOException {
    KeyOutputStream kos = spy(KeyOutputStream.class);
    doNothing().when(kos).write(any(ByteBuffer.class), anyInt(), anyInt());

    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.position(4).limit(12);
    kos.write(buf);

    verify(kos).write(same(buf), eq(4), eq(8));
    assertEquals(12, buf.position(), "position must advance to limit");
    assertFalse(buf.hasRemaining());
  }
}
