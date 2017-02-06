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

import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.hadoop.hive.druid.directory.LuceneDirectoryService;

import java.io.IOException;

/**
 * Only support ingesting expired data, cannot search
 */
public class ExpiredDirectory extends RealtimeDirectoryBase
{
    public ExpiredDirectory (
        SegmentIdentifier segmentIdentifier,
        FieldMappings fieldMappings,
        RealtimeTuningConfig realtimeTuningConfig,
        LuceneDirectoryService directoryService,
        LuceneDocumentBuilder docBuilder
    ) throws IOException {
        super(
            docBuilder,
            segmentIdentifier,
            realtimeTuningConfig,
            fieldMappings,
            directoryService,
            null
        );
    }


}