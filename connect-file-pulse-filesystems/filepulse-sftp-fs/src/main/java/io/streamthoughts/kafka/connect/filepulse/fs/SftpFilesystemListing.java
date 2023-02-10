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
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.reader.SftpFileStorage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SftpFilesystemListing implements FileSystemListing<SftpFileStorage> {
    private static final Logger LOG = LoggerFactory.getLogger(SftpFilesystemListing.class);

    private FileListFilter filter; // TODO

    private SftpFilesystemListingConfig config;

    private ChannelSftp sftp;

    public SftpFilesystemListing(final List<FileListFilter> filters) {
        Objects.requireNonNull(filters, "filters can't be null");
        this.filter = new CompositeFileListFilter(filters);
    }

    public SftpFilesystemListing() {
        this(Collections.emptyList());
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        LOG.info("[SFTP] Configuring SftpFilesystemListing");
        config = new SftpFilesystemListingConfig(configs);
        sftp = SftpClientUtils.initChannelSftp(config);
    }


    @SuppressWarnings("unchecked")
    private Stream<ChannelSftp.LsEntry> sftpList() throws SftpException {
        return sftp.ls(config.getSftpListingDirectoryPath()).stream();
    }

    @Override
    public Collection<FileObjectMeta> listObjects() {
        try {
            LOG.debug("[SFTP] Listing objects in sftp ");
            Stream<GenericFileObjectMeta> ss = sftpList()
                    .filter(e -> !e.getFilename().equals(".") && !e.getFilename().equals(".."))
                            .map(lsEntry -> new GenericFileObjectMeta.Builder()
                                        .withName(lsEntry.getFilename())
                                        .withUri(SftpClientUtils.buildUri(config, lsEntry.getFilename()))
                                        .withLastModified(Instant.ofEpochSecond(lsEntry.getAttrs().getMTime()))
                                        .withContentLength(lsEntry.getAttrs().getSize())
                                        .build())
                            .peek(meta -> LOG.trace("Found object '{}'", meta));

            return ss.collect(Collectors.toUnmodifiableList());
        } catch (SftpException e) {
            throw new ConnectFilePulseException("[SFTP] Cannot list directory to: " +
                    config.getSftpListingDirectoryPath(), e);
        }
    }

    @Override
    public void setFilter(FileListFilter filter) {
        this.filter = filter;
    }

    @Override
    public SftpFileStorage storage() {
        return new SftpFileStorage(config, sftp);
    }
}
