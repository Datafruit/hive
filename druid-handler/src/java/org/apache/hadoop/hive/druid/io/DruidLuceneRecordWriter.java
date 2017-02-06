package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.segment.LuceneAppenderator;
import org.apache.hadoop.hive.druid.segment.LuceneWriterConfig;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DruidLuceneRecordWriter extends DruidRecordWriter {
  protected static final Logger LOG = LoggerFactory.getLogger(DruidRecordWriter.class);

  private final DataSchema dataSchema;

  private final Appenderator appenderator;

  private final RealtimeTuningConfig tuningConfig;

  private final Path segmentsDescriptorDir;

  private SegmentIdentifier currentOpenSegment = null;

  private final Integer maxPartitionSize;

  private final FileSystem fileSystem;

  private final Supplier<Committer> committerSupplier;

  public DruidLuceneRecordWriter(
      DataSchema dataSchema,
      RealtimeTuningConfig realtimeTuningConfig,
      DataSegmentPusher dataSegmentPusher,
      int maxPartitionSize,
      final Path segmentsDescriptorsDir,
      final FileSystem fileSystem
  ) {
    super(dataSchema, realtimeTuningConfig, dataSegmentPusher, maxPartitionSize, segmentsDescriptorsDir, fileSystem);
    this.tuningConfig = Preconditions
        .checkNotNull(realtimeTuningConfig, "realtimeTuningConfig is null");
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "data schema is null");
    this.appenderator = new LuceneAppenderator(dataSchema,
        realtimeTuningConfig,
        new LuceneWriterConfig(),
        dataSegmentPusher,
        DruidStorageHandlerUtils.JSON_MAPPER,
        null,
        null,
        new FireDepartmentMetrics(),
        new DataSegmentAnnouncer() {
          public void announceSegment(DataSegment segment) throws IOException {
          }

          public void unannounceSegment(DataSegment segment) throws IOException {
          }

          public void announceSegments(Iterable<DataSegment> segments) throws IOException {
          }

          public void unannounceSegments(Iterable<DataSegment> segments) throws IOException {
          }

          public boolean isAnnounced(DataSegment segment) {
            return false;
          }
        }

    );
    Preconditions.checkArgument(maxPartitionSize > 0, "maxPartitionSize need to be greater than 0");
    this.maxPartitionSize = maxPartitionSize;
    appenderator.startJob(); // maybe we need to move this out of the constructor
    this.segmentsDescriptorDir = Preconditions
        .checkNotNull(segmentsDescriptorsDir, "segmentsDescriptorsDir is null");
    this.fileSystem = Preconditions.checkNotNull(fileSystem, "file system is null");
    committerSupplier = Suppliers.ofInstance(Committers.nil());
  }

  @Override
  public void write(Writable w) throws IOException {
    DruidWritable record = (DruidWritable) w;
    final long timestamp = (long) record.getValue().get(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    final long truncatedTime = (long) record.getValue()
        .get(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);

    InputRow inputRow = new MapBasedInputRow(
        timestamp,
        dataSchema.getParser()
            .getParseSpec()
            .getDimensionsSpec()
            .getDimensionNames(),
        record.getValue()
    );

    try {
      appenderator
          .add(getSegmentIdentifierAndMaybePush(truncatedTime), inputRow, committerSupplier);
    } catch (SegmentNotWritableException e) {
      throw new IOException(e);
    }
  }
}
