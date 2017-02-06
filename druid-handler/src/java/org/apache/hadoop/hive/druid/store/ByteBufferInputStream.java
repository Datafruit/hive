package org.apache.hadoop.hive.druid.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

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

/**
 * A memory-resident {@link IndexInput} implementation.
 *
 * @version $Id: ByteBufferInputStream.java 632120 2008-02-28 21:13:59Z mikemccand $
 */
class ByteBufferInputStream extends IndexInput implements Cloneable {
  static final int BUFFER_SIZE = ByteBufferOutputStream.BUFFER_SIZE;

  private ByteBufferFile file;
  private long length;

  private ByteBuffer currentBuffer;
  private int currentBufferIndex;
  
  private int bufferPosition;
  private long bufferStart;
  private int bufferLength;

  public ByteBufferInputStream(String name, ByteBufferFile f) throws IOException {
    this(name, f, f.length);
  }

  ByteBufferInputStream(String name, ByteBufferFile f, long length) throws IOException {
    super("ByteBufferInputStream(name=" + name + ")");
    this.file = f;
    this.length = length;
    if (length/BUFFER_SIZE >= Integer.MAX_VALUE) {
      throw new IOException("RAMInputStream too large length=" + length + ": " + name);
    }

    // make sure that we switch to the
    // first needed buffer lazily
    currentBufferIndex = -1;
    currentBuffer = null;
  }

  public void close() {
  }

  public long length() {
    return length;
  }


  public byte readByte() throws IOException {
    if (bufferPosition >= bufferLength) {
      currentBufferIndex++;
      switchCurrentBuffer(true);
    }
    currentBuffer.position(bufferPosition++);
    return currentBuffer.get();
  }

  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      if (bufferPosition >= bufferLength) {
        currentBufferIndex++;
        switchCurrentBuffer(true);
      }

      int remainInBuffer = bufferLength - bufferPosition;
      int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
      currentBuffer.position(bufferPosition);
      currentBuffer.get(b, offset, bytesToCopy);
      offset += bytesToCopy;
      len -= bytesToCopy;
      bufferPosition += bytesToCopy;
      currentBuffer.position(bufferPosition);
    }
  }

  private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
    bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
    if (bufferStart > length || currentBufferIndex >= file.numBuffers()) {
      // end of file reached, no more buffers left
      if (enforceEOF)
        throw new IOException("Read past EOF");
      else {
        // Force EOF if a read takes place at this position
        currentBufferIndex--;
        bufferPosition = BUFFER_SIZE;
        currentBuffer.position(bufferPosition);
      }
    } else {
      currentBuffer = file.getBuffer(currentBufferIndex);
      bufferPosition = 0;
      currentBuffer.position(bufferPosition);
      long buflen = length - bufferStart;
      bufferLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int) buflen;
    }
  }

  @Override
  public long getFilePointer() {
    return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (currentBuffer==null || pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
      currentBufferIndex = (int) (pos / BUFFER_SIZE);
      switchCurrentBuffer(false);
    }
    bufferPosition = (int) (pos % BUFFER_SIZE);
    currentBuffer.position(bufferPosition);
  }

  @Override
  public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
    }
    return new ByteBufferInputStream(getFullSliceDescription(sliceDescription), file, offset + length) {
      {
        seek(0L);
      }

      @Override
      public void seek(long pos) throws IOException {
        if (pos < 0L) {
          throw new IllegalArgumentException("Seeking to negative position: " + this);
        }
        super.seek(pos + offset);
      }

      @Override
      public long getFilePointer() {
        return super.getFilePointer() - offset;
      }

      @Override
      public long length() {
        return super.length() - offset;
      }

      @Override
      public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
        return super.slice(sliceDescription, offset + ofs, len);
      }
    };
  }
}
