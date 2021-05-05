/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.dfs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleDriveFileSystem extends FileSystem {

    private static final Logger logger = LoggerFactory.getLogger(GoogleDriveFileSystem.class);

    private static final String READONLY_ERROR_MESSAGE = "Google Drive is read-only.";

    private Drive client;

    private Path workingDirectory;
    private FileStatus[] fileStatuses;
    private final Map<String,FileStatus> fileStatusCache = new HashMap<String,FileStatus>();

    @Override
    public URI getUri() {
        try {
            return new URI("googledrive:///");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {

        FSDataInputStream stream;
        String fileName = path.toUri().getPath();

        Drive client = getClient();

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            // need to get fileId
            client.files().get(fileId).executeMediaAndDownloadTo(out);
        }
    }

    private Drive getClient() {

        if (client != null) {
            return client;
        }

        // read preferred app name
        String googleDriveAppName = this.getConf().get("googleDriveAppName", "Apache/Drill");

        // read access token from config
        logger.info("Reading Google Drive access token from configuration.");
        String googleDriveAccessToken = this.getConf().get("googleDriveAccessToken", "");

        GoogleCredential credential = new GoogleCredential().setAccessToken(googleDriveAccessToken);

        this.client = new Drive.builder(new NetHttpTransport(),
                                        GsonFactory.getDefaultInstance(),
                                        credential)
            .setApplicationName(googleDriveAppName)
            .build();

        return this.client;
    }
}