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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

/**
 * The output stream for Ozone file system.
 *
 * TODO: Make outputStream generic for both rest and rpc clients
 * This class is not thread safe.
 */
public class OzoneFSOutputStream extends OutputStream
        implements Syncable {

  private final OzoneOutputStream outputStream;

  public OzoneFSOutputStream(OzoneOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSOutputStream.write",
        () -> outputStream.write(b));
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSOutputStream.write",
        () -> {
          TracingUtil.getActiveSpan().setAttribute("length", len);
          outputStream.write(b, off, len);
        });
  }

  /**
   * Writes all of the remaining bytes of {@code buf} (from its current position
   * up to its limit) and advances the buffer's position to its limit, per the
   * {@code org.apache.hadoop.fs.ByteBufferWritable} contract. When {@code buf}
   * is a direct buffer and the underlying key stream supports it, the bytes are
   * routed down without first being copied onto the Java heap.
   *
   * <p>Declared here (rather than by implementing {@code ByteBufferWritable})
   * so that this class keeps loading under the Hadoop 2 profile, where that
   * interface is absent; the capability is exposed by
   * {@code CapableOzoneFSOutputStream} which is only used under Hadoop 3.
   */
  public void write(ByteBuffer buf) throws IOException {
    final int off = buf.position();
    final int len = buf.remaining();
    TracingUtil.executeInNewSpan("OzoneFSOutputStream.write",
        () -> {
          TracingUtil.getActiveSpan().setAttribute("length", len);
          outputStream.write(buf, off, len);
        });
    buf.position(buf.limit());
  }

  @Override
  public synchronized void flush() throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSOutputStream.flush",
        outputStream::flush);
  }

  @Override
  public synchronized void close() throws IOException {
    outputStream.close();
  }

  @Override
  public void hflush() throws IOException {
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    TracingUtil.executeInNewSpan("OzoneFSOutputStream.hsync",
        outputStream::hsync);
  }

  protected OzoneOutputStream getWrappedOutputStream() {
    return outputStream;
  }
}
