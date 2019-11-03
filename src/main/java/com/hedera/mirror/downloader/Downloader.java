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

import com.google.common.base.Stopwatch;

import com.hedera.mirror.addressbook.NetworkAddressBook;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.NodeAddress;
import com.hedera.mirror.domain.StreamItem;
import com.hedera.mirror.repository.ApplicationStatusRepository;
import com.hedera.utilities.Utility;

import javassist.NotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class Downloader {
	protected final Logger log = LogManager.getLogger(getClass());

	private final S3AsyncClient s3Client;
	private List<String> nodeAccountIds;
	private final ApplicationStatusRepository applicationStatusRepository;
    private final NetworkAddressBook networkAddressBook;
	private final DownloaderProperties downloaderProperties;
    // Thread pool used one per node during the download process for signatures.
	private final ExecutorService signatureDownloadThreadPool;
	private final MessageChannel verifiedStreamItemsChannel;
    private String lastValidFileName;
    private String lastValidFileHash;

    public Downloader(S3AsyncClient s3Client, ApplicationStatusRepository applicationStatusRepository,
                      NetworkAddressBook networkAddressBook, DownloaderProperties downloaderProperties,
                      MessageChannel verifiedStreamItemsChannel) {
	    this.s3Client = s3Client;
		this.applicationStatusRepository = applicationStatusRepository;
		this.networkAddressBook = networkAddressBook;
		this.downloaderProperties = downloaderProperties;
		this.verifiedStreamItemsChannel = verifiedStreamItemsChannel;
		lastValidFileName = applicationStatusRepository.findByStatusCode(getLastProcessedFileNameKey());
		if (getLastProcessedFileHashKey() != null) {
            lastValidFileHash = applicationStatusRepository.findByStatusCode(getLastProcessedFileHashKey());
        }
		signatureDownloadThreadPool = Executors.newFixedThreadPool(downloaderProperties.getThreads());
		nodeAccountIds = networkAddressBook.load().stream().map(NodeAddress::getId).collect(Collectors.toList());
        Runtime.getRuntime().addShutdownHook(new Thread(signatureDownloadThreadPool::shutdown));
	}

	protected void downloadNextBatch() {
        try {
            if (!downloaderProperties.isEnabled()) {
                return;
            }
            if (Utility.checkStopFile()) {
                log.info("Stop file found");
                return;
            }
            final var sigFilesMap = downloadSigFiles();
            // Verify signature files and download corresponding files of valid signature files
            verifySigsAndDownloadDataFiles(sigFilesMap);
        } catch (Exception e) {
            log.error("Error downloading files", e);
        }
    }

    /**
     * 	Download all sig files (*.rcd_sig for records, *_Balances.csv_sig for balances) with timestamp later than
     * 	lastValid<Type>FileName
     * 	Validate each file with corresponding node's PubKey.
     * 	Put valid files into HashMap<String, List<File>>

     *  @return
     *      key: sig file name
     *      value: a list of sig files with the same name and from different nodes folder;
     */
	private Map<String, List<StreamItem>> downloadSigFiles() throws InterruptedException {
		String s3Prefix = downloaderProperties.getPrefix();
        // foo.rcd < foo.rcd_sig. If we read foo.rcd from application stats, we have to start listing from
        // next to 'foo.rcd_sig'.
        String lastValidSigFileName = lastValidFileName.isEmpty() ? "" : lastValidFileName + "_sig";

		final var sigFilesMap = new ConcurrentHashMap<String, List<StreamItem>>();

		// refresh node account ids
		nodeAccountIds = networkAddressBook.load().stream().map(NodeAddress::getId).collect(Collectors.toList());
		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(nodeAccountIds.size());
		final var totalDownloads = new AtomicInteger();
		/**
		 * For each node, create a thread that will make S3 ListObject requests as many times as necessary to
		 * start maxDownloads download operations.
		 */
		for (String nodeAccountId : nodeAccountIds) {
			tasks.add(Executors.callable(() -> {
				log.debug("Downloading signature files for node {} created after file {}", nodeAccountId, lastValidSigFileName);
				// Get a list of objects in the bucket, 100 at a time
				String prefix = s3Prefix + nodeAccountId + "/";
				Stopwatch stopwatch = Stopwatch.createStarted();

                try {
                    // batchSize (number of items we plan do download in a single batch) times 2 for file + sig
                    final var listSize = (downloaderProperties.getBatchSize() * 2);
                    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                            .bucket(downloaderProperties.getCommon().getBucketName())
                            .prefix(prefix)
                            .delimiter("/")
                            .startAfter(prefix + lastValidSigFileName)
                            .maxKeys(listSize)
                            .build();
                    CompletableFuture<ListObjectsV2Response> response = s3Client.listObjectsV2(listRequest);
                    var pendingDownloads = new ArrayList<PendingDownload>(downloaderProperties.getBatchSize());
                    // Loop through the list of remote files beginning a download for each relevant sig file.
                    for (S3Object content : response.get().contents()) {
                        String s3ObjectKey = content.key();
                        if (s3ObjectKey.endsWith("_sig")) {
                            String fileName = s3ObjectKey.substring(s3ObjectKey.lastIndexOf("/") + 1);
                            StreamItem item = new StreamItem(StreamItem.Type.SIG, fileName, nodeAccountId,
                                    downloaderProperties.getStreamType());
                            pendingDownloads.add(downloadStreamItem(s3ObjectKey, item));
                            totalDownloads.incrementAndGet();
                        }
                    }

					/*
					 * With the list of pending downloads - wait for them to complete and add them to the list
					 * of downloaded signature files.
					 */
					var ref = new Object() {
						int count = 0;
					};
					pendingDownloads.forEach((pd) -> {
						try {
							if (pd.waitForCompletion()) {
								ref.count++;
								String fileName = pd.getStreamItem().getFileName();
								sigFilesMap.putIfAbsent(fileName, Collections.synchronizedList(new ArrayList<>()));
								sigFilesMap.get(fileName).add(pd.getStreamItem());
							}
						} catch (InterruptedException ex) {
							log.error("Failed downloading {} in {}", pd.getS3key(), pd.getStopwatch(), ex);
						}
					});
					if (ref.count > 0) {
						log.info("Downloaded {} signatures for node {} in {}", ref.count, nodeAccountId, stopwatch);
					}
				} catch (Exception e) {
					log.error("Error downloading signature files for node {} after {}", nodeAccountId, stopwatch, e);
				}
			}));
		}

		// Wait for all tasks to complete.
		// invokeAll() does return Futures, but it waits for all to complete (so they're returned in a completed state).
		Stopwatch stopwatch = Stopwatch.createStarted();
		signatureDownloadThreadPool.invokeAll(tasks);
		if (totalDownloads.get() > 0) {
			var rate = (int)(1000000.0 * totalDownloads.get() / stopwatch.elapsed(TimeUnit.MICROSECONDS));
			log.info("Downloaded {} signatures in {} ({}/s)", totalDownloads, stopwatch, rate);
		}
		return sigFilesMap;
	}

	/**
	 * Returns a PendingDownload for which the caller can waitForCompletion() to wait for the download to complete.
	 * This either queues or begins the download (depending on the AWS TransferManager).
	 */
    private PendingDownload downloadStreamItem(String s3ObjectKey, StreamItem streamItem) {
        CompletableFuture<ResponseBytes<GetObjectResponse>> future = s3Client.getObject(
                GetObjectRequest.builder().bucket(downloaderProperties.getCommon().getBucketName()).key(s3ObjectKey).build(),
                AsyncResponseTransformer.toBytes());
        return new PendingDownload(future, s3ObjectKey, streamItem);
    }

    /**
     *  For each group of signature Files with the same file name:
     *  (1) verify that the signature files are signed by corresponding node's PublicKey;
     *  (2) For valid signature files, we compare their Hashes to see if more than 2/3 Hashes matches.
     *  If more than 2/3 Hashes matches, we download the corresponding data file from a node folder which has valid
     *  signature file.
     *  (3) compare the Hash of data file with Hash which has been agreed on by valid signatures, if match, move the
     *  data file into `valid` directory; else download the data file from other valid node folder, and compare the
     *  Hash until find a match one
     * @param sigFilesMap
     */
    private void verifySigsAndDownloadDataFiles(Map<String, List<StreamItem>> sigFilesMap) {
        // reload address book and keys in case it has been updated by RecordFileLogger
        NodeSignatureVerifier verifier = new NodeSignatureVerifier(networkAddressBook);

        List<String> sigFileNames = new ArrayList<>(sigFilesMap.keySet());
        // sort in increasing order of timestamp, so that we process files in the order they are written.
        // It's very important for record and event files because they form immutable linked list by include one file's
        // hash into next file.
        Collections.sort(sigFileNames);

        for (String sigFileName : sigFileNames) {
            if (Utility.checkStopFile()) {
                log.info("Stop file found, stopping");
                return;
            }

            List<StreamItem> sigFiles = sigFilesMap.get(sigFileName);
            boolean valid = false;

            // If the number of sigFiles is not greater than 2/3 of number of nodes, we don't need to verify them
            if (sigFiles == null || !Utility.greaterThanSuperMajorityNum(sigFiles.size(), nodeAccountIds.size())) {
                log.warn("Signature file count does not exceed 2/3 of nodes");
                continue;
            }

            // validSigFiles are signed by node'key and contains the same Hash which has been agreed by more than 2/3 nodes
            Pair<byte[], List<StreamItem>> hashAndValidSigFiles = verifier.verifySignatureFiles(sigFiles);
            final byte[] validHash = hashAndValidSigFiles.getLeft();
            for (StreamItem validSigStreamItem : hashAndValidSigFiles.getRight()) {
                if (Utility.checkStopFile()) {
                    log.info("Stop file found, stopping");
                    return;
                }
                log.debug("Verified signature file matches at least 2/3 of nodes: {}", sigFileName);

                try {
                    StreamItem signedDataStreamItem = downloadSignedDataFile(validSigStreamItem);
                    if (signedDataStreamItem != null && Utility.hashMatch(validHash, signedDataStreamItem)) {
                        log.debug("Downloaded {} corresponding to verified hash", signedDataStreamItem);
                        // Check that file is newer than last valid downloaded file.
                        // Additionally, if the file type uses prevFileHash based linking, verify that new file is next in
                        // the sequence.
                        if (verifyHashChain(signedDataStreamItem)) {
                            log.debug("Verifier hash chain for {} ", signedDataStreamItem);
                            // move the file to the valid directory
                            if (verifiedStreamItemsChannel.send(
                                    MessageBuilder.withPayload(signedDataStreamItem).build())) {
                                log.debug("Successfully sent {}", signedDataStreamItem);
                                if (getLastProcessedFileHashKey() != null) {
                                    lastValidFileHash = Utility.bytesToHex(validHash);
                                }
                                lastValidFileName = signedDataStreamItem.getFileName();
                                valid = true;
                                break;
                            }
                        }
                    } else if (signedDataStreamItem != null) {
                        log.warn("Hash doesn't match the hash contained in valid signature file. Will try to download" +
                                " a file with same timestamp from other nodes and check the Hash: {}", signedDataStreamItem);
                    }
                } catch (Exception e) {
                    log.error("Error downloading data file corresponding to {}", sigFileName, e);
                }
            }

            if (!valid) {
                log.error("File could not be verified by at least 2/3 of nodes: {}", sigFileName);
            }
        }
    }

    /**
     * Verifies that prevFileHash in given {@code file} matches that in application repository.
     */
    protected boolean verifyHashChain(StreamItem streamItem) {
        String bypassMismatch = applicationStatusRepository.findByStatusCode(getBypassHashKey());
        String prevFileHash;
        try {
            prevFileHash = getPrevFileHash(streamItem.getDataBytes().rewind());
        } catch(NotFoundException e) {
            log.warn("{} does not contain valid previous file hash", streamItem, e);
            return false;
        }

        if (StringUtils.isBlank(lastValidFileHash) || lastValidFileHash.equals(prevFileHash) ||
                Utility.hashIsEmpty(prevFileHash) || bypassMismatch.compareTo(streamItem.getFileName()) > 0) {
            return true;
        }

        log.warn("File Hash Mismatch with previous: {}, expected {}, got {}",
                streamItem.getFileName(), lastValidFileHash, prevFileHash);
        return false;
    }

    private StreamItem downloadSignedDataFile(StreamItem sigStreamItem) {
        String fileName = sigStreamItem.getFileName().replace("_sig", "");
        String s3Prefix = downloaderProperties.getPrefix();

		String s3ObjectKey = s3Prefix + sigStreamItem.getNodeAccountId() + "/" + fileName;

        StreamItem dataStreamItem = new StreamItem(StreamItem.Type.PAYLOAD, fileName, sigStreamItem.getNodeAccountId(),
                sigStreamItem.getStreamType());
		try {
			var pendingDownload = downloadStreamItem(s3ObjectKey, dataStreamItem);
			pendingDownload.waitForCompletion();
			if (pendingDownload.isDownloadSuccessful()) {
			    return pendingDownload.getStreamItem();
            } else {
                log.error("Failed downloading {}", dataStreamItem);
            }
		} catch (Exception ex) {
            log.error("Failed downloading {}", dataStreamItem, ex);
		}
        return null;
    }

    protected abstract ApplicationStatusCode getLastProcessedFileNameKey();
    protected abstract ApplicationStatusCode getLastProcessedFileHashKey();
    protected abstract ApplicationStatusCode getBypassHashKey();
    protected abstract String getPrevFileHash(ByteBuffer data) throws NotFoundException;
}
