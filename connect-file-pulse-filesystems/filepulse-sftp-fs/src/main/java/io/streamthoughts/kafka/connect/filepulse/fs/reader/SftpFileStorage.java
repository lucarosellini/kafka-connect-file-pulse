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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpClientUtils;
import io.streamthoughts.kafka.connect.filepulse.fs.SftpFilesystemListingConfig;
import io.streamthoughts.kafka.connect.filepulse.fs.Storage;
import io.streamthoughts.kafka.connect.filepulse.source.FileObjectMeta;
import io.streamthoughts.kafka.connect.filepulse.source.GenericFileObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;

import static io.streamthoughts.kafka.connect.filepulse.fs.SftpClientUtils.sftpFileInputStream;

public class SftpFileStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(SftpFileStorage.class);

    private ChannelSftp sftp;
    private SftpFilesystemListingConfig config;

    public SftpFileStorage(SftpFilesystemListingConfig config, ChannelSftp sftp) {
        this.sftp = sftp;
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    private Stream<ChannelSftp.LsEntry> sftpList(String path) throws SftpException {
        return sftp.ls(path).stream();
    }

    @Override
    public FileObjectMeta getObjectMetadata(URI uri) {
        try {
            LOG.info("[SFTP] Getting object metadata for '{}'", uri);
            return sftpList(uri.toString())
                    .map(entry -> {
                        SftpATTRS attrs = entry.getAttrs();
                        return new GenericFileObjectMeta.Builder()
                                .withLastModified(Instant.ofEpochSecond(attrs.getMTime()))
                                .withContentLength(attrs.getSize())
                                .withName(entry.getFilename())
                                .withUri(SftpClientUtils.buildUri(config, entry.getFilename()))
                                .build();
                    }).findFirst().orElseThrow(() ->
                            new ConnectFilePulseException("[SFTP] Cannot stat file with uri: " + uri));

        } catch (SftpException e) {
            throw new ConnectFilePulseException("[SFTP] Cannot stat file with uri "+uri, e);
        }
    }

    @Override
    public boolean exists(URI uri) {
        try {
            LOG.info("[SFTP] Checking if '{}' exists", uri);
            SftpATTRS attrs = sftp.stat(uri.toString());

            return attrs.isReg() || attrs.isDir();
        } catch (SftpException e) {
            throw new ConnectFilePulseException("[SFTP] Cannot stat file with uri "+uri, e);
        }
    }

    @Override
    public boolean delete(URI uri) {
        throw new ConnectFilePulseException("[SFTP] delete not yet implemented");
    }

    @Override
    public boolean move(URI source, URI dest) {
        throw new ConnectFilePulseException("[SFTP] move not yet implemented");
    }

    @Override
    public InputStream getInputStream(URI uri) throws Exception {
        return sftpFileInputStream(uri);
    }
}
