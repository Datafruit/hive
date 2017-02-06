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

import org.apache.lucene.store.*;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ByteBufferDirectory extends BaseDirectory implements Accountable {

  protected final Map<String,ByteBufferFile> fileMap = new ConcurrentHashMap<>();
  protected final AtomicLong sizeInBytes = new AtomicLong();

  /** Constructs an empty {@link Directory}. */
  public ByteBufferDirectory() {
    this(new SingleInstanceLockFactory());
  }

  /** Constructs an empty {@link Directory} with the given {@link LockFactory}. */
  public ByteBufferDirectory(LockFactory lockFactory) {
    super(lockFactory);
  }

  public ByteBufferDirectory(FSDirectory dir, IOContext context) throws IOException {
    this(dir, false, context);
  }

  private ByteBufferDirectory(FSDirectory dir, boolean closeDir, IOContext context) throws IOException {
    this();
    for (String file : dir.listAll()) {
        copyFrom(dir, file, file, context);
    }
    if (closeDir) {
      dir.close();
    }
  }

  public String[] listAll() {
    ensureOpen();
    Set<String> fileNames = fileMap.keySet();
    List<String> names = new ArrayList<>(fileNames.size());
    for (String name : fileNames) names.add(name);
    return names.toArray(new String[names.size()]);
  }

  public final boolean fileNameExists(String name) {
    ensureOpen();
    return fileMap.containsKey(name);
  }

  /** Returns the length in bytes of a file in the directory.
   * @throws IOException if the file does not exist
   */
  public long fileLength(String name) throws IOException {
    ensureOpen();
    ByteBufferFile file = fileMap.get(name);
    if (file == null) {
      throw new FileNotFoundException(name);
    }
    return file.getLength();
  }

  /**
   * Return total size in bytes of all files in this directory. This is
   * currently quantized to ByteBufferOutputStream.BUFFER_SIZE.
   */
  @Override
  public final long ramBytesUsed() {
    ensureOpen();
    return sizeInBytes.get();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("file", fileMap);
  }

  /** Removes an existing file in the directory.
   * @throws IOException if the file does not exist
   */
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    ByteBufferFile file = fileMap.remove(name);
    if (file != null) {
      file.directory = null;
      sizeInBytes.addAndGet(-file.sizeInBytes);
    } else {
      throw new FileNotFoundException(name);
    }
  }

  /** Creates a new, empty file in the directory with the given name. Returns a stream writing this file. */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    ByteBufferFile file = newByteBufferFile();
    ByteBufferFile existing = fileMap.remove(name);
    if (existing != null) {
      sizeInBytes.addAndGet(-existing.sizeInBytes);
      existing.directory = null;
    }
    fileMap.put(name, file);
    return new ByteBufferOutputStream(name, file, true);
  }

  /**
   * Returns a new {@link ByteBufferFile} for storing data. This method can be
   * overridden to return different {@link ByteBufferFile} impls, that e.g. override
   * {@link ByteBufferFile#newBuffer(int)}.
   */
  protected ByteBufferFile newByteBufferFile() {
    return new ByteBufferFile(this);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
  }


  /** Renames an existing file in the directory.
   * @throws FileNotFoundException if from does not exist
   * @deprecated
   */
  public void renameFile(String source, String dest) throws IOException {
    ensureOpen();
    ByteBufferFile file = fileMap.get(source);
    if (file == null) {
      throw new FileNotFoundException(source);
    }
    fileMap.put(dest, file);
    fileMap.remove(source);
  }

  /** Returns a stream reading an existing file. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ByteBufferFile file = fileMap.get(name);
    if (file == null) {
      throw new FileNotFoundException(name);
    }
    return new ByteBufferInputStream(name, file);
  }

  /** Closes the store to future operations, releasing associated memory. */
  public void close() {
    isOpen = false;
    fileMap.clear();
  }
}
