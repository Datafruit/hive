package org.apache.hadoop.hive.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.druid.directory.LuceneDirectory;
import org.apache.hadoop.hive.druid.directory.LuceneDirectoryService;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class RealtimeDirectoryBase implements LuceneDirectory
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeDirectoryBase.class);

  private volatile int numRowsAdded;
  private int numRowsPersisted;
  private volatile boolean isOpen = true;

  private final File basePersistDir;
  private final File finalDir;
  private final File tempPersistDirFile;

  private final LuceneDocumentBuilder docBuilder;
  private final SegmentIdentifier segmentIdentifier;
  private final Map<String, DimensionSchema> dimensions;
  private final LuceneDirectoryService directoryService;

  private final IndexWriter realtimeWriter;
  private final FieldMappings fieldMappings;

  private final Lock refreshLock = new ReentrantLock();

  public Lock getRefreshLock() {
    return refreshLock;
  }

  public RealtimeDirectoryBase(
      LuceneDocumentBuilder docBuilder,
      SegmentIdentifier segmentIdentifier,
      RealtimeTuningConfig realtimeTuningConfig,
      FieldMappings fieldMappings,
      LuceneDirectoryService directoryService,
      Directory realtimeDirectory
  ) throws IOException
  {
    this.segmentIdentifier = segmentIdentifier;
    this.basePersistDir = realtimeTuningConfig.getBasePersistDirectory();
    this.tempPersistDirFile = computeDir("temp");
    this.finalDir = computeDir("");
    this.docBuilder = docBuilder;

    this.fieldMappings = fieldMappings;
    this.directoryService = directoryService;
    this.dimensions = fieldMappings.getFieldTypes();
    Directory directory = realtimeDirectory == null
                        ? FSDirectory.open(tempPersistDirFile.toPath())
                        : realtimeDirectory;
    realtimeWriter = directoryService.newRealtimeWriter(directory);
    reset();
  }

  public IndexWriter getRealtimeWriter()
  {
    return realtimeWriter;
  }

  @Override
  public DirectoryReader getIndexReader() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseIndexReader(DirectoryReader directoryReader) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void refreshRealtimeReader() throws IOException {
    throw new UnsupportedOperationException();
  }

  public void ensureOpen()
  {
    if (!isOpen) {
      throw new AlreadyClosedException("directory already closed");
    }
  }

  public boolean isOpen()
  {
    return isOpen;
  }

  @Override
  public int numRows()
  {
    return numRowsPersisted + numRowsAdded;
  }

  public void add(InputRow row) throws IOException, ParseException
  {
    ensureOpen();
    realtimeWriter.addDocument(docBuilder.buildLuceneDocument(row));
    numRowsAdded++;
  }

  public File getBasePersistDir()
  {
    return finalDir;
  }

  public DataSegment getSegment()
  {
    return new DataSegment(
        segmentIdentifier.getDataSource(),
        segmentIdentifier.getInterval(),
        segmentIdentifier.getVersion(),
        ImmutableMap.<String, Object>of(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        segmentIdentifier.getShardSpec(),
        null,
        numRows()
    );
  }

  @Override
  public Map<String, DimensionSchema> getFieldTypes() {
    return dimensions;
  }

  public void reset() throws IOException {
    if (isOpen) {
      return;
    }

    numRowsPersisted += numRowsAdded;
    numRowsAdded = 0;
  }

  @Override
  public SegmentIdentifier getIdentifier()
  {
    return segmentIdentifier;
  }

  public File merge() throws IOException
  {
    log.info("The directory [%s] merge records count is :" + numRows(), segmentIdentifier.getIdentifierAsString());
    if (realtimeWriter.isOpen()) {
      realtimeWriter.commit();

      refreshLock.lock();
      try {
        realtimeWriter.close();
      } catch (IOException ex) {
        throw new IOException(ex);
      } finally {
        refreshLock.unlock();
      }

      log.info("Start to merge lucene segments...");
      long timeStamp = System.currentTimeMillis();

      IndexWriter finalWriter = directoryService.newFinalWriter(finalDir.toPath());
      finalWriter.addIndexes(realtimeWriter.getDirectory());
      finalWriter.forceMerge(1);
      finalWriter.commit();
      finalWriter.close();
      log.info("Merge lucene segments is completed, expend time: "
          + (System.currentTimeMillis()-timeStamp) +"ms,"
          + ", file count: " + finalDir.list().length
      );
    }
    //write .mapping file for fields type
    fieldMappings.writeFieldTypesTo(finalDir);
    return finalDir;
  }

  protected void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) { }
    }
  }


  private File computeDir(String prefix) throws IOException {
    String fileName = prefix + segmentIdentifier.toString().replaceAll(":", "");
    File file = new File(basePersistDir, fileName);
    if (file.exists()) {
      log.warn("delete exsist dir: " + file.toString());
      FileUtils.deleteDirectory(file);
    }
    FileUtils.forceMkdir(file);
    return file;
  }

  @Override
  public void close() throws IOException {
    ensureOpen();
    isOpen =false;

    removeDirectory(tempPersistDirFile);
  }
}
