/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.druid.segment;

import com.metamx.emitter.EmittingLogger;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.hadoop.hive.druid.directory.LuceneDirectoryService;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

/**
 */
public class RealtimeDirectory extends RealtimeDirectoryBase
{
    private static final EmittingLogger log = new EmittingLogger(RealtimeDirectory.class);

    private final static Double RAM_BUFFER_SIZE_MB = 5000.0;

    private final ReaderManager readerManager;
    private final IndexWriter realtimeWriter;

    public RealtimeDirectory(
            final SegmentIdentifier segmentIdentifier,
            final FieldMappings fieldMappings,
            final RealtimeTuningConfig realtimeTuningConfig,
            final LuceneDirectoryService directoryService,
            final LuceneDocumentBuilder docBuilder
    ) throws IOException {
        super(
            docBuilder,
            segmentIdentifier,
            realtimeTuningConfig,
            fieldMappings,
            directoryService,
            null
        );
        realtimeWriter = getRealtimeWriter();
        readerManager = new ReaderManager(realtimeWriter, false);
        readerManager.addListener(new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() throws IOException {
            }

            @Override
            public void afterRefresh(boolean didRefresh) throws IOException {
                if (!didRefresh) {
                    log.warn("Refresh real time index fail, segment id: " + getIdentifier());
                }
            }
        });
    }

    /**
     * Refreshes the internal index reader on a scheduled thread.
     * This is a controlled thread so it is ok to block this thread to make sure things are in a good state.
     * @throws IOException
     */
    @Override
    public void refreshRealtimeReader() throws IOException {
        if (realtimeWriter.isOpen()) {
            getRefreshLock().lock();
            try {
                readerManager.maybeRefresh();
            } catch (IOException ex) {
            } finally {
                getRefreshLock().unlock();
            }
        }
    }


    /**
     * Gets an index reader for search. This can be accessed by multiple threads and cannot be blocking.
     * @return an index reader containing in memory realtime index as well as persisted indexes. Null
     * if the index is either closed or has no documents yet indexed.
     * @throws IOException
     */
    @Override
    public DirectoryReader getIndexReader() throws IOException {
        if (!isOpen()) {
            return null;
        }

        return readerManager.acquire();
    }

    @Override
    public void releaseIndexReader(DirectoryReader directoryReader) throws IOException {
        readerManager.release(directoryReader);
    }


    @Override
    public void close() throws IOException {
        ensureOpen();
        DirectoryReader directoryReader = readerManager.acquire();
        directoryReader.decRef();
        directoryReader.close();
        super.close();

    }
}