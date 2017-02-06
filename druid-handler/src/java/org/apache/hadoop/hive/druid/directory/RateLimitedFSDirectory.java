/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.druid.directory;

import org.apache.lucene.store.*;

import java.io.IOException;

public final class RateLimitedFSDirectory extends FilterDirectory
{
    private final StoreRateLimiting.Listener rateListener;

    private final StoreRateLimiting storeRateLimiting;

    public RateLimitedFSDirectory(
            Directory wrapped,
            StoreRateLimiting.Provider rateLimitingProvider,
            StoreRateLimiting.Listener rateListener
    )
    {
        super(wrapped);
        this.rateListener = rateListener;
        this.storeRateLimiting = rateLimitingProvider.rateLimiting();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException
    {
        final IndexOutput output = super.createOutput(name, context);
        if (!storeRateLimiting.isUseRateLimit()) {
            return output;
        } else {
            RateLimiter limiter = storeRateLimiting.getRateLimiter();
            return new RateLimitedIndexOutput(new RateLimiterWrapper(limiter, rateListener), output);
        }
    }


    @Override
    public void close() throws IOException
    {
        in.close();
    }

    @Override
    public String toString()
    {
        if (!storeRateLimiting.isUseRateLimit()) {
            return toString(in);
        } else {
            return "rate_limited(" + toString(in) + ", rate=" + storeRateLimiting.getRateLimiter().getMBPerSec() + ")";
        }
    }

    private String toString(Directory directory)
    {
        if (directory instanceof NIOFSDirectory) {
            NIOFSDirectory niofsDirectory = (NIOFSDirectory)directory;
            return "niofs(" + niofsDirectory.getDirectory() + ")";
        }
        if (directory instanceof MMapDirectory) {
            MMapDirectory mMapDirectory = (MMapDirectory)directory;
            return "mmapfs(" + mMapDirectory.getDirectory() + ")";
        }
        if (directory instanceof SimpleFSDirectory) {
            SimpleFSDirectory simpleFSDirectory = (SimpleFSDirectory)directory;
            return "simplefs(" + simpleFSDirectory.getDirectory() + ")";
        }
        if (directory instanceof FileSwitchDirectory) {
            FileSwitchDirectory fileSwitchDirectory = (FileSwitchDirectory) directory;
            return "default(" + toString(fileSwitchDirectory.getPrimaryDir()) + "," + toString(fileSwitchDirectory.getSecondaryDir()) + ")";
        }

        return directory.toString();
    }

    // we wrap the limiter to notify our store if we limited to get statistics
    static final class RateLimiterWrapper extends RateLimiter {
        private final RateLimiter delegate;
        private final StoreRateLimiting.Listener rateListener;

        RateLimiterWrapper(RateLimiter delegate, StoreRateLimiting.Listener rateListener) {
            this.delegate = delegate;
            this.rateListener = rateListener;
        }

        @Override
        public void setMBPerSec(double mbPerSec) {
            delegate.setMBPerSec(mbPerSec);
        }

        @Override
        public double getMBPerSec() {
            return delegate.getMBPerSec();
        }

        @Override
        public long pause(long bytes) throws IOException {
            long pause = delegate.pause(bytes);
            rateListener.onPause(pause);
            return pause;
        }

        @Override
        public long getMinPauseCheckBytes() {
            return delegate.getMinPauseCheckBytes();
        }
    }

}
