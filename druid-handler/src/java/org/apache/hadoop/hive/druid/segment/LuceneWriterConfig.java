package org.apache.hadoop.hive.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.lucene.index.IndexWriterConfig;

/**
 *
 */
public class LuceneWriterConfig
{
    private final static long DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS = 3;

    private final static boolean DEFAULT_INDEX_MERGE = true;
    private final static double DEFAULT_RAM_BUFFER_SIZE_MB = 16;
    private final static int DEFAULT_MaxMergeAtOnce = 5;
    private final static int DEFAULT_MaxMergedSegmentMB = 256;
    private final static int DEFAULT_MaxMergesThreads = 1;

    private final static double DEFAULT_RATELIMITER_MB_PER_SECOND = 0.0;

    public LuceneWriterConfig()
    {
        this(
                IndexWriterConfig.DISABLE_AUTO_FLUSH,
                DEFAULT_RAM_BUFFER_SIZE_MB,
                DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS,
                DEFAULT_INDEX_MERGE,
                DEFAULT_MaxMergeAtOnce,
                DEFAULT_MaxMergedSegmentMB,
                DEFAULT_MaxMergesThreads,
                DEFAULT_RATELIMITER_MB_PER_SECOND
        );
    }

    @JsonCreator
    public LuceneWriterConfig(
            @JsonProperty("maxBufferedDocs") Integer maxBufferedDocs,
            @JsonProperty("ramBufferSizeMB") Double ramBufferSizeMB,
            @JsonProperty("indexRefreshIntervalSeconds") Long indexRefreshIntervalSeconds,
            @JsonProperty("isIndexMerge") Boolean isIndexMerge,
            @JsonProperty("maxMergeAtOnce") Integer maxMergeAtOnce,
            @JsonProperty("maxMergedSegmentMB") Integer maxMergedSegmentMB,
            @JsonProperty("maxMergesThreads") Integer maxMergesThreads,
            @JsonProperty("limiterMBPerSec") Double limiterMBPerSec
    )
    {
        this.maxBufferedDocs = maxBufferedDocs == null ? IndexWriterConfig.DISABLE_AUTO_FLUSH : maxBufferedDocs;
        this.ramBufferSizeMB = ramBufferSizeMB == null ? DEFAULT_RAM_BUFFER_SIZE_MB : ramBufferSizeMB;
        this.indexRefreshIntervalSeconds = indexRefreshIntervalSeconds == null ? DEFAULT_INDEX_REFRESH_INTERVAL_SECONDS : indexRefreshIntervalSeconds;
        this.isIndexMerge = isIndexMerge == null ? DEFAULT_INDEX_MERGE : isIndexMerge;
        this.maxMergeAtOnce = maxMergeAtOnce == null ? DEFAULT_MaxMergeAtOnce : maxMergeAtOnce;
        this.maxMergedSegmentMB = maxMergedSegmentMB == null ? DEFAULT_MaxMergedSegmentMB : maxMergedSegmentMB;
        this.maxMergesThreads = maxMergesThreads == null ? DEFAULT_MaxMergesThreads : maxMergesThreads;
        this.limiterMBPerSec = limiterMBPerSec;
    }


    private int maxBufferedDocs;

    private Double ramBufferSizeMB;

    private long indexRefreshIntervalSeconds;

    private boolean isIndexMerge;

    private int maxMergeAtOnce;

    private int maxMergedSegmentMB;

    private int maxMergesThreads;

    private double limiterMBPerSec;

    @JsonProperty
    public int getMaxBufferedDocs()
    {
        return maxBufferedDocs;
    }

    @JsonProperty
    public Double getRamBufferSizeMB()
    {
        return ramBufferSizeMB;
    }

    @JsonProperty
    public long getIndexRefreshIntervalSeconds()
    {
        return indexRefreshIntervalSeconds;
    }

    @JsonProperty("isIndexMerge")
    public boolean isIndexMerge()
    {
        return isIndexMerge;
    }

    @JsonProperty
    public int getMaxMergeAtOnce()
    {
        return maxMergeAtOnce;
    }

    @JsonProperty
    public int getMaxMergedSegmentMB()
    {
        return maxMergedSegmentMB;
    }

    @JsonProperty
    public int getMaxMergesThreads()
    {
        return maxMergesThreads;
    }

    @JsonProperty
    public double getLimiterMBPerSec() {
        return limiterMBPerSec;
    }

    public void setMaxBufferedDocs(int maxBufferedDocs) {
        this.maxBufferedDocs = maxBufferedDocs;
    }

    public void setRamBufferSizeMB(Double ramBufferSizeMB) {
        this.ramBufferSizeMB = ramBufferSizeMB;
    }

    public void setIndexRefreshIntervalSeconds(long indexRefreshIntervalSeconds) {
        this.indexRefreshIntervalSeconds = indexRefreshIntervalSeconds;
    }

    public void setIndexMerge(boolean indexMerge) {
        isIndexMerge = indexMerge;
    }

    public void setMaxMergeAtOnce(int maxMergeAtOnce) {
        this.maxMergeAtOnce = maxMergeAtOnce;
    }

    public void setMaxMergedSegmentMB(int maxMergedSegmentMB) {
        this.maxMergedSegmentMB = maxMergedSegmentMB;
    }

    public void setMaxMergesThreads(int maxMergesThreads) {
        this.maxMergesThreads = maxMergesThreads;
    }

    public void setLimiterMBPerSec(double limiterMBPerSec) {
        this.limiterMBPerSec = limiterMBPerSec;
    }
}
