/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs.iterator;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.IteratorManager;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.NonBlockingBufferReader;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileWithFooterInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class SftpRowFileInputIteratorBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SftpRowFileInputIteratorBuilder.class);

    private Charset charset = StandardCharsets.UTF_8;
    private int minNumReadRecords = 1;
    private FileObjectMeta metadata;
    private long waitMaxMs = 0;
    private int skipHeaders = 0;
    private int skipFooters = 0;
    private IteratorManager iteratorManager;
    private Supplier<NonBlockingBufferReader> readerSupplier;

    public SftpRowFileInputIteratorBuilder withReaderSupplier(final Supplier<NonBlockingBufferReader> readerSupplier) {
        this.readerSupplier = readerSupplier;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withMetadata(final FileObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withSkipHeaders(final int skipHeaders) {
        this.skipHeaders = skipHeaders;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withSkipFooters(final int skipFooters) {
        this.skipFooters = skipFooters;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withMinNumReadRecords(final int minNumReadRecords) {
        this.minNumReadRecords = minNumReadRecords;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withMaxWaitMs(final long maxWaitMs) {
        this.waitMaxMs = maxWaitMs;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withCharset(final Charset charset) {
        this.charset = charset;
        return this;
    }

    public SftpRowFileInputIteratorBuilder withIteratorManager(final IteratorManager iteratorManager) {
        this.iteratorManager = iteratorManager;
        return this;
    }

    public FileInputIterator<FileRecord<TypedStruct>> build() {
        FileInputIterator<FileRecord<TypedStruct>> iterator;

        LOG.info("[SFTP] Building iterator");

        NonBlockingBufferReader reader = readerSupplier.get();

        iterator = new RowFileInputIterator(metadata, iteratorManager, reader)
                .setMinNumReadRecords(minNumReadRecords)
                .setMaxWaitMs(waitMaxMs);

        if (skipFooters > 0) {
            iterator = new RowFileWithFooterInputIterator(
                    skipFooters,
                    new File(metadata.uri()),
                    charset,
                    iterator
            );
        }

        if (skipHeaders > 0) {
            iterator = new SftpRowFileWithHeadersInputIterator(
                    skipHeaders,
                    reader,
                    iterator
            );
        }

        return iterator;
    }
}
