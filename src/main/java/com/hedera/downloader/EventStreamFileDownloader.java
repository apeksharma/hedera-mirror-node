package com.hedera.downloader;

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

import com.hedera.configLoader.ConfigLoader;
import com.hedera.configLoader.ConfigLoader.OPERATION_TYPE;
import com.hedera.mirror.config.DownloaderProperties;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.repository.ApplicationStatusRepository;
import com.hedera.mirror.config.EventProperties;
import com.hedera.parser.EventStreamFileParser;
import com.hedera.signatureVerifier.NodeSignatureVerifier;
import com.hedera.utilities.Utility;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Named;
import java.io.File;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j2
@Named
public class EventStreamFileDownloader extends Downloader {

	private final String validDir = ConfigLoader.getDefaultParseDir(OPERATION_TYPE.EVENTS);
	private final String tmpDir = ConfigLoader.getDefaultTmpDir(OPERATION_TYPE.EVENTS);
	private final EventProperties eventProperties;

	public EventStreamFileDownloader(ApplicationStatusRepository applicationStatusRepository, EventProperties eventProperties, DownloaderProperties downloaderProperties) {
		super(applicationStatusRepository, eventProperties.getDownloader(), downloaderProperties);
		this.eventProperties = eventProperties;
		Utility.ensureDirectory(validDir);
		Utility.ensureDirectory(tmpDir);
		Utility.purgeDirectory(tmpDir);
	}

	@Scheduled(fixedRateString = "${hedera.mirror.event.downloader.frequency:100}")
	public void download() {
		if (!eventProperties.isEnabled()) {
			return;
		}

		if (Utility.checkStopFile()) {
			log.info("Stop file found");
			return;
		}

		try {
			if (Utility.checkStopFile()) {
				log.info("Stop file found, exiting");
				return;
			}

			final var sigFilesMap = downloadSigFiles(DownloadType.EVENT);

			// Verify signature files and download .evts files of valid signature files
			verifySigsAndDownloadEventStreamFiles(sigFilesMap);
			verifyValidFiles();
		} catch (Exception e) {
			log.error("Error downloading and verifying new event files", e);
		}
	}

	/**
	 * Check if there is any missing .evts file:
	 * (1) Sort .evts files by timestamp,
	 * (2) Verify the .evts files to see if the file Hash matches prevFileHash
	 *
	 * @throws Exception 
	 */
	private void verifyValidFiles() throws Exception {
		String lastValidEventFileName = applicationStatusRepository.findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE);
		String lastValidEventFileHash = applicationStatusRepository.findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE_HASH);
		
		String lastValidEventFileName2 = lastValidEventFileName;
		try (Stream<Path> pathStream = Files.walk(Paths.get(validDir))) {
			List<String> fileNames = pathStream.filter(p -> Utility.isEventStreamFile(p.toString()))
					.filter(p -> lastValidEventFileName2.isEmpty() ||
							fileNameComparator.compare(p.toFile().getName(), lastValidEventFileName2) > 0)
					.sorted(pathComparator)
					.map(p -> p.toString()).collect(Collectors.toList());

			String newLastValidEventFileName = lastValidEventFileName2;
			String newLastValidEventFileHash = lastValidEventFileHash;

			for (String fileName : fileNames) {
				String prevFileHash = EventStreamFileParser.readPrevFileHash(fileName);
				if (prevFileHash == null) {
					log.info("{} doesn't contain valid prevFileHash", fileName);
					break;
				}
				if (newLastValidEventFileHash.isEmpty() ||
						newLastValidEventFileHash.equals(prevFileHash) ||
						prevFileHash.equals(Hex.encodeHexString(new byte[48]))) {
					newLastValidEventFileHash = Utility.bytesToHex(Utility.getFileHash(fileName));
					newLastValidEventFileName = new File(fileName).getName();
				} else {
					break;
				}
			}

			if (!newLastValidEventFileName.equals(lastValidEventFileName)) {
				applicationStatusRepository.updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE_HASH, newLastValidEventFileHash);
				applicationStatusRepository.updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE, newLastValidEventFileName);
			}

		} catch (Exception ex) {
			log.error("Failed to verify event files in {}", validDir, ex);
		}
	}

	/**
	 * For each group of signature Files with the same file name:
	 * (1) verify that the signature files are signed by corresponding node's PublicKey;
	 * (2) For valid signature files, we compare their Hashes to see if more than 2/3 Hashes matches.
	 * If more than 2/3 Hashes matches, we download the corresponding .evts file from a node folder which has valid
	 * signature file.
	 * (3) compare the Hash of .evts file with Hash which has been agreed on by valid signatures, if match, move the
	 * .evts file into `valid` directory; else download .evts file from other valid node folder, and compare the Hash
	 * until find a match one
	 * return the name of directory which contains valid .evts files
	 *
	 * @param sigFilesMap
	 */
	private void verifySigsAndDownloadEventStreamFiles(Map<String, List<File>> sigFilesMap) {

		NodeSignatureVerifier verifier = new NodeSignatureVerifier();
		for (String fileName : sigFilesMap.keySet()) {
			boolean valid = false;
			List<File> sigFiles = sigFilesMap.get(fileName);

			// If the number of sigFiles is not greater than 2/3 of number of nodes, we don't need to verify them
			if (sigFiles == null || !Utility.greaterThanSuperMajorityNum(sigFiles.size(), nodeAccountIds.size())) {
				log.warn("Signature file count does not exceed 2/3 of nodes");
				continue;
			}

			// validSigFiles are signed by node key and contains the same hash which has been agreed by more than 2/3
			List<File> validSigFiles = verifier.verifySignatureFiles(sigFiles);

			for (File validSigFile : validSigFiles) {
				Pair<Boolean, File> fileResult = downloadFile(DownloadType.EVENT, validSigFile, tmpDir);
				File file = fileResult.getRight();
				if (file != null &&	Utility.hashMatch(validSigFile, file)) {
					log.debug("Verified signature file matches at least 2/3 of nodes: {}", fileName);
					// move the file to the valid directory
					File fTo = new File(validDir + file.getName());

					if (moveFile(file, fTo)) {
						log.debug("Verified signature file matches at least 2/3 of nodes: {}", fileName);
						valid = true;
						break;
					}
				} else if (file != null) {
					log.warn("Hash of {} doesn't match the hash contained in the signature file. Will try to download a event file with same timestamp from other nodes", file);
				}
			}

			if (!valid) {
				log.error("File could not be verified by at least 2/3 of nodes: {}", fileName);
			}
		}
	}
}
