package com.hedera.mirror.importer.parser;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.hedera.mirror.importer.exception.ParserException;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;

import com.hedera.mirror.importer.util.ShutdownHelper;
import com.hedera.mirror.importer.util.Utility;

@RequiredArgsConstructor
public abstract class FileWatcher {

    protected final Logger log = LogManager.getLogger(getClass());
    protected final ParserProperties parserProperties;

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void watch() {
        Path path = parserProperties.getValidPath();
        if (!parserProperties.isEnabled()) {
            log.info("Skip watching directory: {}", path);
            return;
        }

        // Invoke on startup to check for any changed files while this process was down.
        onCreate();

        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            log.info("Watching directory for changes: {}", path);
            WatchKey rootKey = path
                    .register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            boolean valid = rootKey.isValid();

            while (valid && parserProperties.isEnabled()) {
                WatchKey key;
                try {
                    key = watcher.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    continue;
                }

                if (ShutdownHelper.isStopping()) {
                    return;
                }

                if (key == null) {
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        log.error("File watching events may have been lost or discarded");
                        continue;
                    }

                    onCreate();
                }

                valid = key.reset();
            }
        } catch (Exception e) {
            log.error("Error starting watch service", e);
        }
    }

    public interface FileProcessor {
        boolean process(String fileName, InputStream is) throws ParserException;
    }

    protected void listAndProcessAllFiles(FileProcessor fileProcessor) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Path validDir = parserProperties.getValidPath();
            log.debug("Parsing files from {}", validDir);
            String[] unsortedFiles = validDir.toFile().list(); // get all files under the valid directory
            Arrays.sort(unsortedFiles);           // sorted by name (timestamp)

            // add directory prefix to get full path
            List<File> sortedFiles = Arrays.stream(unsortedFiles)
                    .filter(this::isDataFile)
                    .map(s -> validDir.resolve(s).toFile())
                    .collect(Collectors.toList());

            processAllFiles(sortedFiles, fileProcessor);
        } catch (Exception e) {
            log.error("Error processing balances files after {}", stopwatch, e);
        }
    }

    protected void processAllFiles(List<File> files, FileProcessor fileProcessor)
            throws IOException, ParserException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (File file : files) {
            if (ShutdownHelper.isStopping()) {
                throw new RuntimeException("Process is shutting down");
            }
            if (!file.exists()) {
                log.warn("File does not exist : {}", file);
                return ;
            }
            log.trace("Processing file: {}", file);
            if (fileProcessor.process(file.getName(), new FileInputStream(file))) {
                // move it
                Utility.moveFileToParsedDir(file.getCanonicalPath(), parserProperties.getParsedPath());
            }
            // TODO: add config 'ignoreBadFiles'. true => continue parsing. false => block parsing
            // true for balances, false for record file
        }
        log.info("Completed processing {} files in {}", files.size(), stopwatch);
    }

    public abstract void onCreate();

    /**
     * @return true if the file represents data file, and not the sig file.
     */
    public abstract boolean isDataFile(String filename);
}
