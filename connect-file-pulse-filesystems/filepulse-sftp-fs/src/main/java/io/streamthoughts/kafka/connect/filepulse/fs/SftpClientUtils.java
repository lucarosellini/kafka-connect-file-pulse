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
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import io.streamthoughts.kafka.connect.filepulse.errors.ConnectFilePulseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SftpClientUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SftpClientUtils.class);

    private static ChannelSftp sftp;

    /**
     * Manages the sftp channel as a singleton object.
     *
     * @param config the connector config.
     * @return the newly instantiated or cached sftp channel.
     */
    public static ChannelSftp initChannelSftp(SftpFilesystemListingConfig config) {
        if (sftp != null) {
            LOG.info("[SFTP] SFTP channel already open");
            return sftp;
        }

        LOG.info("[SFTP] Opening SFTP channel");

        JSch jsch = new JSch();

        try {
            Session jschSession = jsch.getSession(config.getSftpListingUser(), config.getSftpListingHost(),
                    config.getSftpListingPort());
            jschSession.setPassword(config.getSftpListingPassword());
            jschSession.setConfig("StrictHostKeyChecking", config.getSftpListingStrictHostKeyCheck());

            jschSession.connect(10000);
            sftp = (ChannelSftp) jschSession.openChannel("sftp");
            sftp.connect(5000);
            LOG.info("[SFTP] SFTP channel opened successfully.");
            return sftp;
        } catch (JSchException e) {
            throw new ConnectFilePulseException(String.format("[SFTP] Cannot connect as user %s to %s:%d",
                    config.getSftpListingUser(),
                    config.getSftpListingHost(),
                    config.getSftpListingPort()), e);
        }
    }

    /**
     * Builds a file URI as the absolute path from the sftp root.
     *
     * @param config the connector config object.
     * @param name the filename.
     * @return the absolute path of the file
     */
    public static URI buildUri(SftpFilesystemListingConfig config, String name) {
        return URI.create(String.format("%s/%s",
                config.getSftpListingDirectoryPath(),
                name));
    }

    /**
     * Wraps the raw stream into the appropriate specific compressed stream.
     * @param absolutePath the absolute file path.
     * @param raw the underlying input stream.
     * @return the wrapped input stream if appropriate, the raw stream otherwise.
     * @throws IOException if the file is compressed but for some reason the compressed stream cannot be created.
     */
    private static InputStream wrapCompressedStream(String absolutePath, InputStream raw) throws IOException {
        if (absolutePath.toLowerCase().endsWith("zip")) {
            LOG.debug("[SFTP] Input file is a ZIP, embedding inputstream into a ZipInputStream");
            ZipInputStream zipInputStream = new ZipInputStream(raw, StandardCharsets.UTF_8);

            ZipEntry zipEntry = zipInputStream.getNextEntry();

            if (zipEntry != null) {
                return zipInputStream;
            } else {
                throw new ConnectFilePulseException(String.format("Zip file '%s' has no content", absolutePath));
            }
        } else {
            return raw;
        }
    }

    /**
     * Instantiates an input stream on the remote file on the sftp.
     *
     * @param uri the URI representing a file on the SFTP
     * @return the inputstream.
     */
    public static InputStream sftpFileInputStream(URI uri) {
        LOG.debug("[SFTP] Getting input stream for '{}'", uri);
        String absolutePath = uri.toString();

        try {
            return wrapCompressedStream(absolutePath, sftp.get(absolutePath));
        } catch (SftpException e) {
            LOG.error("[SFTP] Cannot open sftp InputStream for " + absolutePath, e);
            throw new ConnectFilePulseException(e);
        } catch (IOException ioe) {
            LOG.error("[SFTP] Cannot wrap InputStream into a compressed stream for " + absolutePath, ioe);
            throw new ConnectFilePulseException(ioe);
        }
    }
}
