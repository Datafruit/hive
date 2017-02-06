package org.apache.hadoop.hive.druid.directory;

import org.apache.lucene.store.RateLimiter;

public class StoreRateLimiting
{
    public interface Provider
    {
        StoreRateLimiting rateLimiting();
    }

    public interface Listener
    {
        void onPause(long nanos);
    }

    private final RateLimiter.SimpleRateLimiter rateLimiter = new RateLimiter.SimpleRateLimiter(0);
    private volatile RateLimiter.SimpleRateLimiter actualRateLimiter;

    public RateLimiter getRateLimiter()
    {
        return actualRateLimiter;
    }

    public boolean isUseRateLimit()
    {
        return getRateLimiter() != null;
    }

    public void setMaxRate(double rate)
    {
        if (rate <= 0) {
            actualRateLimiter = null;
        } else if (actualRateLimiter == null) {
            actualRateLimiter = rateLimiter;
            actualRateLimiter.setMBPerSec(rate);
        } else {
            assert rateLimiter == actualRateLimiter;
            rateLimiter.setMBPerSec(rate);
        }
    }
}
