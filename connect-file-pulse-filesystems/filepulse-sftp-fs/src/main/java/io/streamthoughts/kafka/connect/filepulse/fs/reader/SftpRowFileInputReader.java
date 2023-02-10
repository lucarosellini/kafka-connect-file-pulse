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
package io.streamthoughts.kafka.connect.filepulse.fs.reader;

import io.streamthoughts.kafka.connect.filepulse.data.TypedStruct;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpClientUtils;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.iterator.SftpRowFileInputIteratorFactory;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.text.RowFileInputIteratorConfig;
import io.streamthoughts.kafka.connect.filepulse.reader.FileInputIterator;
import io.streamthoughts.kafka.connect.filepulse.reader.StorageAwareFileInputReader;
import io.streamthoughts.kafka.connect.filepulse.source.FileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

public class SftpRowFileInputReader extends AbstractFileInputReader
        implements StorageAwareFileInputReader<SftpFileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(SftpRowFileInputReader.class);

    private SftpFileStorage storage;

    private SftpRowFileInputIteratorFactory factory;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        LOG.debug("[SFTP] Configuring SftpRowFileInputReader");

        if (storage == null) {
            final SftpFilesystemListingConfig config = new SftpFilesystemListingConfig(configs);
            storage = new SftpFileStorage(config, SftpClientUtils.initChannelSftp(config));
            LOG.debug("[SFTP] Storage instantiated successfully");
        }

        this.factory = new SftpRowFileInputIteratorFactory(
                new RowFileInputIteratorConfig(configs),
                storage,
                iteratorManager()
        );
    }

    @Override
    public SftpFileStorage storage() {
        return storage;
    }

    @Override
    protected FileInputIterator<FileRecord<TypedStruct>> newIterator(URI objectURI, IteratorManager iteratorManager) {
        LOG.info("[SFTP] Getting new iterator for object '{}'", objectURI);
        return factory.newIterator(objectURI);
    }
}
