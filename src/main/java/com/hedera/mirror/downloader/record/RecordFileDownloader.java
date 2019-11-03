package com.hedera.mirror.downloader.record;

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

import com.hedera.mirror.addressbook.NetworkAddressBook;
import com.hedera.mirror.domain.StreamItem;
import com.hedera.mirror.downloader.Downloader;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.repository.ApplicationStatusRepository;
import com.hedera.mirror.parser.record.RecordFileParser;

import javassist.NotFoundException;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.inject.Named;
import java.nio.ByteBuffer;

@Log4j2
@Named
public class RecordFileDownloader extends Downloader {

    public RecordFileDownloader(
            S3AsyncClient s3Client, ApplicationStatusRepository applicationStatusRepository,
            NetworkAddressBook networkAddressBook, RecordDownloaderProperties downloaderProperties,
            MessageChannel verifiedRecordStreamItemChannel) {
        super(s3Client, applicationStatusRepository, networkAddressBook, downloaderProperties,
                verifiedRecordStreamItemChannel);
    }

    @Scheduled(fixedRateString = "${hedera.mirror.downloader.record.frequency:500}")
    public void download() {
        downloadNextBatch();
    }

    protected ApplicationStatusCode getLastProcessedFileNameKey() {
        return ApplicationStatusCode.LAST_PROCESSED_RECORD_FILENAME;
    }

    protected ApplicationStatusCode getLastProcessedFileHashKey() {
        return ApplicationStatusCode.LAST_PROCESSED_RECORD_HASH;
    }

    protected ApplicationStatusCode getBypassHashKey() {
        return ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER;
    }

    protected String getPrevFileHash(ByteBuffer data) throws NotFoundException {
        return RecordFileParser.readPrevFileHash(data);
    }
}
