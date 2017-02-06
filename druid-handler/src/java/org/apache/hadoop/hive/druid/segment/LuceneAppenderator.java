package org.apache.hadoop.hive.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.*;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.*;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.druid.common.CRC32Utils;
import org.apache.hadoop.hive.druid.directory.LuceneDirectoryService;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class LuceneAppenderator implements Appenderator
{
  private static final EmittingLogger log = new EmittingLogger(LuceneAppenderator.class);

  private final DataSchema schema;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final RealtimeTuningConfig realtimeTuningConfig;
  private final ExecutorService queryExecutorService;

  private volatile boolean isClosed = false;
  private volatile ScheduledExecutorService indexRefreshExecutor = null;
  private final Map<SegmentIdentifier, RealtimeDirectoryBase> directories = Maps.newConcurrentMap();
  private final List<RealtimeDirectoryBase> toRefreshDirectories = Lists.newArrayList();
  private final Set<SegmentIdentifier> droppingDirectories = new ConcurrentHashSet();
  private final VersionedIntervalTimeline<String, RealtimeDirectoryBase> timeline = new VersionedIntervalTimeline<>(
      Ordering.natural()
  );

  //  private volatile long nextFlush;
  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService mergeExecutor = null;
  private final boolean reportParseExceptions;
  private final FireDepartmentMetrics metrics;
  private final LuceneWriterConfig luceneWriterConfig;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final FieldMappings fieldMappings;
  private final LuceneDocumentBuilder luceneDocumentBuilder;
  private final LuceneDirectoryService directoryService;

  public LuceneAppenderator(
      DataSchema schema,
      RealtimeTuningConfig realtimeTuningConfig,
      LuceneWriterConfig luceneWriterConfig,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService queryExecutorService,
      FireDepartmentMetrics metrics,
      DataSegmentAnnouncer segmentAnnouncer
  )
  {
    this.schema = schema;
    this.realtimeTuningConfig = realtimeTuningConfig;
    this.luceneWriterConfig = luceneWriterConfig;
    this.reportParseExceptions = realtimeTuningConfig.isReportParseExceptions();
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.objectMapper = objectMapper;
    this.conglomerate = conglomerate;
    this.metrics = metrics;
    this.segmentAnnouncer = segmentAnnouncer;
    this.fieldMappings = FieldMappings.builder().buildFieldTypesFrom(schema).build();
    this.luceneDocumentBuilder = new LuceneDocumentBuilder(fieldMappings);
    this.directoryService = new LuceneDirectoryService(luceneWriterConfig);
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public Object startJob()
  {
    initializeExecutors();
    refreshIndex();
    return null;
  }

  @Override
  public int add(SegmentIdentifier identifier, InputRow row,
                 Supplier<Committer> committerSupplier) throws IndexSizeExceededException,
      SegmentNotWritableException
  {

    try {
      RealtimeDirectoryBase directory = getOrCreateDir(identifier);
      directory.add(row);
      return directory.numRows();
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new SegmentNotWritableException(ioe.getMessage(), ioe);
    }
  }

  private RealtimeDirectoryBase getOrCreateDir(final SegmentIdentifier identifier) throws IOException
  {
    RealtimeDirectoryBase retVal = directories.get(identifier);

    if (retVal == null) {
        log.info("expired directory: " + identifier.getDataSource()+"_"+identifier.getInterval());
        retVal = new ExpiredDirectory(identifier, fieldMappings, realtimeTuningConfig, directoryService, luceneDocumentBuilder);
      }

    //just for pusher read
    Files.asByteSink(new File(retVal.getBasePersistDir(), "version.bin")).write(Ints.toByteArray(0x9));

    directories.put(identifier, retVal);

    return retVal;
  }

  @Override
  public List<SegmentIdentifier> getSegments() {
    return ImmutableList.copyOf(directories.keySet());
  }


  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query,
                                                       Iterable<Interval> intervals)
  {
    return getQueryRunnerForSegments(query, getSegmentDescriptorFromTimeLine(intervals));
  }

  private List<SegmentDescriptor> getSegmentDescriptorFromTimeLine(Iterable<Interval> intervals)
  {
    final List<SegmentDescriptor> specs = Lists.newArrayList();

    Iterables.addAll(
        specs,
        FunctionalIterable
            .create(intervals)
            .transformCat(
                new Function<Interval, Iterable<TimelineObjectHolder<String, RealtimeDirectoryBase>>>()
                {
                  @Override
                  public Iterable<TimelineObjectHolder<String, RealtimeDirectoryBase>> apply(final Interval interval)
                  {
                    return timeline.lookup(interval);
                  }
                }
            )
            .transformCat(
                new Function<TimelineObjectHolder<String, RealtimeDirectoryBase>, Iterable<SegmentDescriptor>>()
                {
                  @Override
                  public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, RealtimeDirectoryBase> holder)
                  {
                    return FunctionalIterable
                        .create(holder.getObject())
                        .transform(
                            new Function<PartitionChunk<RealtimeDirectoryBase>, SegmentDescriptor>()
                            {
                              @Override
                              public SegmentDescriptor apply(final PartitionChunk<RealtimeDirectoryBase> chunk)
                              {
                                return new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    chunk.getChunkNumber()
                                );
                              }
                            }
                        );
                  }
                }
            )
    );
    return specs;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query,
                                                      Iterable<SegmentDescriptor> specs)
  {
    // We only handle one dataSource. Make sure it's in the list of names, then ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
          .addData("dataSource", query.getDataSource())
          .emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
          .addData("dataSource", query.getDataSource())
          .emit();
      return new NoopQueryRunner<>();
    }

    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(specs)
                .transform(
                    new Function<SegmentDescriptor, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                      {
                        final PartitionHolder<RealtimeDirectoryBase> holder = timeline.findEntry(
                            descriptor.getInterval(),
                            descriptor.getVersion()
                        );
                        if (holder == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final PartitionChunk<RealtimeDirectoryBase> chunk = holder.getChunk(descriptor.getPartitionNumber());
                        if (chunk == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final RealtimeDirectoryBase directory = chunk.getObject();

                        return new SpecificSegmentQueryRunner<>(
                            new BySegmentQueryRunner<>(
                                directory.getIdentifier().getIdentifierAsString(),
                                descriptor.getInterval().getStart(),
                                factory.createRunner(new LuceneIncrementalSegment(directory))
                            ),
                            new SpecificSegmentSpec(descriptor)
                        );
                      }
                    }
                )
        )
    );
  }

  @Override
  public int getRowCount(SegmentIdentifier identifier)
  {
    RealtimeDirectoryBase directory = directories.get(identifier);
    return directory == null ? 0 : directory.numRows();
  }

  @Override
  public void clear() throws InterruptedException
  {
    // Drop commit metadata, then abandon all segments.

    try {
      final ListenableFuture<?> uncommitFuture = persistExecutor.submit(
          new Callable<Object>()
          {
            @Override
            public Object call() throws Exception
            {
              objectMapper.writeValue(computeCommitFile(), Committed.nil());
              return null;
            }
          }
      );

      // Await uncommit.
      uncommitFuture.get();

      // Drop everything.
      final List<ListenableFuture<?>> futures = Lists.newArrayList();
      for (Map.Entry<SegmentIdentifier, RealtimeDirectoryBase> entry : directories.entrySet()) {
        futures.add(abandonSegment(entry.getKey(), entry.getValue(), true));
      }

      // Await dropping.
      Futures.allAsList(futures).get();
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    } finally {
      removeDirectory(realtimeTuningConfig.getBasePersistDirectory());
    }
  }

  @Override
  public ListenableFuture<?> drop(SegmentIdentifier identifier)
  {
    final RealtimeDirectoryBase directory = directories.get(identifier);
    if (directory != null) {
      return abandonSegment(identifier, directory, true);
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persistAll(final Committer committer)
  {
    final Map<SegmentIdentifier, Integer> commitHydrants = Maps.newHashMap();
    final String threadName = String.format("%s-incremental-persist", schema.getDataSource());
    final Object commitMetadata = committer.getMetadata();
    final ListenableFuture<Object> future = persistExecutor.submit(
        new ThreadRenamingCallable<Object>(threadName)
        {
          @Override
          public Object doCall()
          {
            try {
              committer.run();
              objectMapper.writeValue(computeCommitFile(), Committed.create(commitHydrants, commitMetadata));

              return commitMetadata;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

//    resetNextFlush();
    return future;
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    final Map<SegmentIdentifier, RealtimeDirectoryBase> dirs = Maps.newHashMap();
    for (final SegmentIdentifier identifier : identifiers) {
      final RealtimeDirectoryBase directory = directories.get(identifier);
      if (directory == null) {
        throw new NullPointerException("No sink for identifier: " + identifier);
      }
      dirs.put(identifier, directory);
    }

    return Futures.transform(
        persistAll(committer),
        new Function<Object, SegmentsAndMetadata>()
        {
          @Override
          public SegmentsAndMetadata apply(Object commitMetadata)
          {
            final List<DataSegment> dataSegments = Lists.newArrayList();

            for (Map.Entry<SegmentIdentifier, RealtimeDirectoryBase> entry : dirs.entrySet()) {
              if (droppingDirectories.contains(entry.getKey())) {
                log.info("Skipping push of currently-dropping sink[%s]", entry.getKey());
                continue;
              }

              final DataSegment dataSegment = mergeAndPush(entry.getValue());
              if (dataSegment != null) {
                dataSegments.add(dataSegment);
              } else {
                log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
              }
            }

            return new SegmentsAndMetadata(dataSegments, commitMetadata);
          }
        },
        mergeExecutor
    );
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> mergeBarrier()
  {
    return mergeExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            // Do nothing
          }
        }
    );
  }

  private DataSegment mergeAndPush(final RealtimeDirectoryBase directory)
  {
    try {
      File mergedFile = directory.merge();
      String crc32 = CRC32Utils.getCRC32(mergedFile);
      Files.asByteSink(new File(mergedFile, CRC32Utils.CRC_NAME)).write(crc32.getBytes());
      DataSegment segment = dataSegmentPusher.push(
          mergedFile,
          directory.getSegment()
      );
      return segment;
    } catch (Exception e) {
      log.warn(e, "Failed to push merged index for segment[%s].", directory.getIdentifier());
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close()
  {
    log.info("Shutting down...");

    final List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (Map.Entry<SegmentIdentifier, RealtimeDirectoryBase> entry : directories.entrySet()) {
      futures.add(abandonSegment(entry.getKey(), entry.getValue(), false));
    }

    try {
      Futures.allAsList(futures).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(e, "Interrupted during close()");
    }
    catch (ExecutionException e) {
      log.warn(e, "Unable to abandon existing segments during close()");
    }

    isClosed = true;
    try {
      shutdownExecutors();
      Preconditions.checkState(persistExecutor.awaitTermination(1, TimeUnit.DAYS), "persistExecutor not terminated");
      Preconditions.checkState(mergeExecutor.awaitTermination(1, TimeUnit.DAYS), "mergeExecutor not terminated");
      Preconditions.checkState(indexRefreshExecutor.awaitTermination(1, TimeUnit.DAYS), "indexRefreshExecutor not terminated");
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }
  }

  private void initializeExecutors()
  {
    final int maxPendingPersists = realtimeTuningConfig.getMaxPendingPersists();

    if (indexRefreshExecutor == null) {
      indexRefreshExecutor = Execs.scheduledSingleThreaded("index_refresh_%d");
    }
    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_persist_%d", maxPendingPersists
          )
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_merge_%d", 1
          )
      );
    }
  }

  private void shutdownExecutors()
  {
    persistExecutor.shutdownNow();
    mergeExecutor.shutdownNow();
    indexRefreshExecutor.shutdownNow();
  }

//  private void resetNextFlush()
//  {
//    nextFlush = new DateTime().plus(realtimeTuningConfig.getIntermediatePersistPeriod()).getMillis();
//  }


  private ListenableFuture<?> abandonSegment(
      final SegmentIdentifier identifier,
      final RealtimeDirectoryBase directory,
      final boolean removeOnDiskData
  )
  {
    // Mark this identifier as dropping, so no future merge tasks will pick it up.
    droppingDirectories.add(identifier);

    // Wait for any outstanding merges to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
        mergeBarrier(),
        new Function<Object, Object>()
        {
          @Nullable
          @Override
          public Object apply(@Nullable Object input)
          {
            if (directories.get(identifier) != directory) {
              // Only abandon sink if it is the same one originally requested to be abandoned.
              log.warn("Sink for segment[%s] no longer valid, not abandoning.", identifier);
              return null;
            }

            if (removeOnDiskData) {
              // Remove this segment from the committed list. This must be done from the persist thread.
              log.info("Removing commit metadata for segment[%s].", identifier);
              try {
                final File commitFile = computeCommitFile();
                if (commitFile.exists()) {
                  final Committed oldCommitted = objectMapper.readValue(commitFile, Committed.class);
                  objectMapper.writeValue(commitFile, oldCommitted.without(identifier.getIdentifierAsString()));
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                    .addData("identifier", identifier.getIdentifierAsString())
                    .emit();
                throw Throwables.propagate(e);
              }
            }
            if (directory instanceof RealtimeDirectory) {
              try {
                segmentAnnouncer.unannounceSegment(directory.getSegment());
              } catch (Exception e) {
                log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                    .addData("identifier", identifier.getIdentifierAsString())
                    .emit();
              }
            }

            log.info("Removing sink for segment[%s].", identifier);
            directories.remove(identifier);
            droppingDirectories.remove(identifier);
            timeline.remove(
                directory.getIdentifier().getInterval(),
                directory.getIdentifier().getVersion(),
                identifier.getShardSpec().createChunk(directory)
            );

            try {
              if (directory.isOpen()) {
                directory.close();
              }

              if (removeOnDiskData) {
                removeDirectory(directory.getBasePersistDir());
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                  .addData("identifier", identifier.getIdentifierAsString())
                  .emit();
            }

            return null;
          }
        },
        persistExecutor
    );
  }

  private void refreshIndex() {
    indexRefreshExecutor.execute(new Runnable() {
      @Override
      public void run() {
        while(!isClosed) {
          log.debug("refresh index segments");
          for (RealtimeDirectoryBase directory : toRefreshDirectories) {
            try {
              directory.refreshRealtimeReader();
            } catch (IOException e) {
              log.error(e.getMessage(), e);
            }
          }

          try {
            Thread.sleep(luceneWriterConfig.getIndexRefreshIntervalSeconds() * 1000);   // refresh eery
          } catch (InterruptedException ie) {
            continue;
          }
        }
      }
    });
  }

  private File computeCommitFile()
  {
    return new File(realtimeTuningConfig.getBasePersistDirectory(), "commit.json");
  }

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
            .addData("file", target)
            .emit();
      }
    }
  }
}

