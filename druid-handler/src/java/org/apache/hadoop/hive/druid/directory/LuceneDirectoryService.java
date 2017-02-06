package org.apache.hadoop.hive.druid.directory;

import com.metamx.emitter.EmittingLogger;
import org.apache.hadoop.hive.druid.segment.LuceneWriterConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.hadoop.hive.druid.store.ByteBufferDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class LuceneDirectoryService implements StoreRateLimiting.Provider, StoreRateLimiting.Listener
{
    private static final EmittingLogger log = new EmittingLogger(LuceneDirectoryService.class);

    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final LuceneWriterConfig luceneWriterConfig;

    public LuceneDirectoryService(LuceneWriterConfig luceneWriterConfig)
    {
        this.luceneWriterConfig= luceneWriterConfig;
        rateLimiting.setMaxRate(luceneWriterConfig.getLimiterMBPerSec());
    }

    @Override
    public void onPause(long nanos)
    {
        log.info("pause " + nanos + " ns");
    }

    @Override
    public StoreRateLimiting rateLimiting()
    {
        return this.rateLimiting;
    }

    public LuceneDirectoryService withRamBufferSizeMB(double ramBufferSizeMB)
    {
        LuceneWriterConfig newLuceneWriterConfig = new LuceneWriterConfig();
        newLuceneWriterConfig.setRamBufferSizeMB(ramBufferSizeMB);
        return new LuceneDirectoryService(newLuceneWriterConfig);
    }


    public Directory newDirectory(Directory directory) throws IOException {
        if (rateLimiting.isUseRateLimit()) {
            return new RateLimitedFSDirectory(directory, this, this);
        } else {
            return directory;
        }
    }

    public IndexWriter newFinalWriter(Path path) throws IOException {
        FSDirectory directory = FSDirectory.open(path);
        return new IndexWriter(newDirectory(directory), initIndexWriterConfig());
    }

    public IndexWriter newRealtimeWriter() throws IOException {
        ByteBufferDirectory directory = new ByteBufferDirectory();
        return newRealtimeWriter(directory);
    }

    public IndexWriter newRealtimeWriter(Directory directory) throws IOException {
        IndexWriterConfig writerConfig = initIndexWriterConfig(new StandardAnalyzer());
        return new IndexWriter(newDirectory(directory), writerConfig);
    }

    public IndexWriter newPersistWriter(Path path) throws IOException {
        FSDirectory directory = FSDirectory.open(path);
        return newPersistWriter(directory);
    }

    public IndexWriter newPersistWriter(Directory directory) throws IOException {
        IndexWriterConfig writerConfig = initIndexWriterConfig(new StandardAnalyzer());
        if (!luceneWriterConfig.isIndexMerge()) {
            writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        } else {
            TieredMergePolicy mergePolicy = (TieredMergePolicy)writerConfig.getMergePolicy();
            mergePolicy.setMaxMergeAtOnce(luceneWriterConfig.getMaxMergeAtOnce());
            mergePolicy.setMaxMergedSegmentMB(luceneWriterConfig.getMaxMergedSegmentMB());

            MergeScheduler ms = writerConfig.getMergeScheduler();
            if (ms instanceof ConcurrentMergeScheduler) {
                ((ConcurrentMergeScheduler) ms).setMaxMergesAndThreads(luceneWriterConfig.getMaxMergeAtOnce(),luceneWriterConfig.getMaxMergesThreads());
            }
        }
        return new IndexWriter(newDirectory(directory), writerConfig);
    }


    protected IndexWriterConfig initIndexWriterConfig()
    {
        return initIndexWriterConfig(null);
    }

    protected IndexWriterConfig initIndexWriterConfig(Analyzer analyzer)
    {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setUseCompoundFile(false);
        writerConfig.setRAMBufferSizeMB(luceneWriterConfig.getRamBufferSizeMB());
        writerConfig.setMaxBufferedDocs(luceneWriterConfig.getMaxBufferedDocs());
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writerConfig.setCommitOnClose(false);
        return writerConfig;
    }

}
