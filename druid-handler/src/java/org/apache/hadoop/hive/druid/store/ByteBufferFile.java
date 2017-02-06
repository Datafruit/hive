package org.apache.hadoop.hive.druid.store;


import org.apache.lucene.util.Accountable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ByteBufferFile implements Accountable {
    protected ArrayList<ByteBuffer> buffers = new ArrayList<>();
    long length;
    ByteBufferDirectory directory;
    protected long sizeInBytes;

    // File used as buffer, in no RAMDirectory
    protected ByteBufferFile() {

    }

    ByteBufferFile(ByteBufferDirectory directory) {
        this.directory = directory;
    }

    // For non-stream access from thread that might be concurrent with writing
    public long getLength() {
        return length;
    }

    protected void setLength(long length) {
        this.length = length;
    }

    protected final ByteBuffer addBuffer(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        synchronized (this) {
            buffers.add(buffer);
            sizeInBytes += size;
        }

        if (directory != null) {
            directory.sizeInBytes.getAndAdd(size);
        }
        return buffer;
    }

    protected ByteBuffer getBuffer(int index) {
        return buffers.get(index);
    }

    protected int numBuffers() {
        return buffers.size();
    }

    /**
     * Expert: allocate a new buffer.
     * Subclasses can allocate differently.
     * @param size size of allocated buffer.
     * @return allocated buffer.
     */
    protected byte[] newBuffer(int size) {
        return new byte[size];
    }

    @Override
    public synchronized long ramBytesUsed() {
        return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(length=" + length + ")";
    }

    @Override
    public int hashCode() {
        int h = (int) (length ^ (length >>> 32));
        for (ByteBuffer block : buffers) {
            h = 31 * h + Arrays.hashCode(block.array());
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ByteBufferFile other = (ByteBufferFile) obj;
        if (length != other.length) return false;
        if (buffers.size() != other.buffers.size()) {
            return false;
        }
        for (int i = 0; i < buffers.size(); i++) {
            if (!Arrays.equals(buffers.get(i).array(), other.buffers.get(i).array())) {
                return false;
            }
        }
        return true;
    }
}
