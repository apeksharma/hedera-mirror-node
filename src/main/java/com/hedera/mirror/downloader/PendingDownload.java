package com.hedera.mirror.downloader;

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

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.transfer.Download;
import com.google.common.base.Stopwatch;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.amazonaws.services.s3.transfer.Transfer.TransferState.Completed;

/**
 * The results of a pending download from the AWS TransferManager.
 * Call waitForCompletion() to wait for the transfer to complete and get the status of whether it was successful
 * or not.
 */
@Log4j2
@Value
public class PendingDownload {
	final Download s3Download;
	final CompletableFuture<Path> gcpDownload;
	Stopwatch stopwatch;
	File file; // Destination file
	String objectKey; // Source S3/GCP key
	@NonFinal boolean alreadyWaited = false; // has waitForCompletion been called
	@NonFinal boolean downloadSuccessful;

    public PendingDownload(final Download s3Download, final File file, final String objectKey) {
        this.s3Download = s3Download;
        this.gcpDownload = null;
        this.stopwatch = Stopwatch.createStarted();
        this.file = file;
        this.objectKey = objectKey;
    }

    public PendingDownload(final CompletableFuture<Path> gcpDownload, final File file, final String objectKey) {
        this.s3Download = null;
        this.gcpDownload = gcpDownload;
        this.stopwatch = Stopwatch.createStarted();
        this.file = file;
        this.objectKey = objectKey;
    }

	/**
	 * @return true if the download was successful.
	 * @throws InterruptedException
	 */
	public boolean waitForCompletion() throws InterruptedException {
		if (alreadyWaited) {
			return downloadSuccessful;
		}
		alreadyWaited = true;
        try {
            if (s3Download != null) {
                s3Download.waitForCompletion();
                if (s3Download.isDone() && (Completed == s3Download.getState())) {
                    log.debug("Finished downloading {} in {}", objectKey, stopwatch);
                    downloadSuccessful = true;
                } else {
                    log.error("Failed downloading {} after {}", objectKey, stopwatch);
                    downloadSuccessful = false;
                }
            } else {
                gcpDownload.get();
                log.debug("Finished downloading {} in {}", objectKey, stopwatch);
                downloadSuccessful = true;
            }
        } catch (InterruptedException e) {
            log.error("Failed downloading {} after {}", objectKey, stopwatch, e);
            downloadSuccessful = false;
            throw e;
        } catch (SdkBaseException | ExecutionException ex) {
            log.error("Failed downloading {} after {}", objectKey, stopwatch, ex);
            downloadSuccessful = false;
        }
        return downloadSuccessful;
	}
}
