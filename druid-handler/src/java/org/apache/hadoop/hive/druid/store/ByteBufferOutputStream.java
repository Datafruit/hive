package org.apache.hadoop.hive.druid.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * A memory-resident {@link IndexOutput} implementation.
 * 
 * <p>For Lucene internal use</p>
 * @version $Id: RAMOutputStream.java 941125 2010-05-05 00:44:15Z mikemccand $
 */

public class ByteBufferOutputStream extends IndexOutput implements Accountable {
  static final int BUFFER_SIZE = 1024 * 1024;

  private ByteBufferFile file;

  private ByteBuffer currentBuffer;
  private int currentBufferIndex;

  private int bufferPosition;
  private long bufferStart;
  private int bufferLength;

  private final Checksum crc;

  /** Construct an empty output buffer. */
  public ByteBufferOutputStream() {
    this("noname", new ByteBufferFile(), false);
  }

  /** Creates this, with no name. */
  public ByteBufferOutputStream(ByteBufferFile f, boolean checksum) {
    this("noname", f, checksum);
  }

  /** Creates this, with specified name. */
  public ByteBufferOutputStream(String name, ByteBufferFile f, boolean checksum) {
    super("ByteBufferOutputStream(name=" + name + "\")");
    file = f;

    // make sure that we switch to the
    // first needed buffer lazily
    currentBufferIndex = -1;
    currentBuffer = null;
    if (checksum) {
      crc = new BufferedChecksum(new CRC32());
    } else {
      crc = null;
    }
  }

  /** Copy the current contents of this buffer to the named output. */
  public void writeTo(DataOutput out) throws IOException {
    flush();
    final long end = file.length;
    long pos = 0;
    int buffer = 0;
    while (pos < end) {
      int length = BUFFER_SIZE;
      long nextPos = pos + length;
      if (nextPos > end) {                        // at the last buffer
        length = (int)(end - pos);
      }
      out.writeBytes(file.getBuffer(buffer++).array(), length);
      pos = nextPos;
    }
  }

  /** Copy the current contents of this buffer to output
   *  byte array */
  public void writeTo(byte[] bytes, int offset) throws IOException {
    flush();
    final long end = file.length;
    long pos = 0;
    int buffer = 0;
    int bytesUpto = offset;
    while (pos < end) {
      int length = BUFFER_SIZE;
      long nextPos = pos + length;
      if (nextPos > end) {                        // at the last buffer
        length = (int)(end - pos);
      }
      System.arraycopy(file.getBuffer(buffer++), 0, bytes, bytesUpto, length);
      bytesUpto += length;
      pos = nextPos;
    }
  }

  /** Resets this to an empty file. */
  public void reset() {
    currentBuffer = null;
    currentBufferIndex = -1;
    bufferPosition = 0;
    bufferStart = 0;
    bufferLength = 0;
    file.setLength(0);
    if (crc != null) {
      crc.reset();
    }
  }

  public void close() throws IOException {
    flush();
  }

  public void writeByte(byte b) throws IOException {
    if (bufferPosition == bufferLength) {
      currentBufferIndex++;
      switchCurrentBuffer();
    }
    if (crc != null) {
      crc.update(b);
    }
    currentBuffer.position(bufferPosition++);
    currentBuffer.put(b);
  }

  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    assert b != null;
    if (crc != null) {
      crc.update(b, offset, len);
    }
    while (len > 0) {
      if (bufferPosition ==  bufferLength) {
        currentBufferIndex++;
        switchCurrentBuffer();
      }

      int remainInBuffer = currentBuffer.capacity() - bufferPosition;
      int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;

      currentBuffer.position(bufferPosition);
      currentBuffer.put(b, offset, bytesToCopy);

      offset += bytesToCopy;
      len -= bytesToCopy;
      bufferPosition += bytesToCopy;
      currentBuffer.position(bufferPosition);
    }
  }

  private void switchCurrentBuffer() throws IOException {
    if (currentBufferIndex == file.numBuffers()) {
      currentBuffer = file.addBuffer(BUFFER_SIZE);
    } else {
      currentBuffer = file.getBuffer(currentBufferIndex);
    }
    bufferPosition = 0;
    currentBuffer.position(bufferPosition);
    bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
    bufferLength = currentBuffer.capacity();
  }

  private void setFileLength() {
    long pointer = bufferStart + bufferPosition;
    if (pointer > file.length) {
      file.setLength(pointer);
    }
  }

  public void flush() throws IOException {
    setFileLength();
  }

  public long getFilePointer() {
    return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
  }

  /** Returns byte usage of all buffers. */
  @Override
  public long ramBytesUsed() {
    return (long) file.numBuffers() * (long) BUFFER_SIZE;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.singleton(Accountables.namedAccountable("file", file));
  }

  @Override
  public long getChecksum() throws IOException {
    if (crc == null) {
      throw new IllegalStateException("internal RAMOutputStream created with checksum disabled");
    } else {
      return crc.getValue();
    }
  }
}