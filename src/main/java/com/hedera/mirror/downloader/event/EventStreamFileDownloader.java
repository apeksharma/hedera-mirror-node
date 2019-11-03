package com.hedera.mirror.downloader.event;

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
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.downloader.Downloader;
import com.hedera.mirror.repository.ApplicationStatusRepository;
import com.hedera.mirror.parser.event.EventStreamFileParser;

import javassist.NotFoundException;
import lombok.extern.log4j.Log4j2;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.inject.Named;
import java.nio.ByteBuffer;

@Log4j2
@Named
public class EventStreamFileDownloader extends Downloader {

    public EventStreamFileDownloader(
            S3AsyncClient s3Client, ApplicationStatusRepository applicationStatusRepository,
            NetworkAddressBook networkAddressBook, EventDownloaderProperties downloaderProperties,
            MessageChannel verifiedEventStreamItemChannel) {
        super(s3Client, applicationStatusRepository, networkAddressBook, downloaderProperties,
                verifiedEventStreamItemChannel);
    }

    @Scheduled(fixedRateString = "${hedera.mirror.downloader.event.frequency:60000}")
    public void download() {
        downloadNextBatch();
    }

    protected ApplicationStatusCode getLastProcessedFileNameKey() {
        return ApplicationStatusCode.LAST_PROCESSED_EVENT_FILENAME;
    }

    protected ApplicationStatusCode getLastProcessedFileHashKey() {
        return ApplicationStatusCode.LAST_PROCESSED_EVENT_HASH;
    }

    protected ApplicationStatusCode getBypassHashKey() {
        return ApplicationStatusCode.EVENT_HASH_MISMATCH_BYPASS_UNTIL_AFTER;
    }

    protected String getPrevFileHash(ByteBuffer data) throws NotFoundException {
        return EventStreamFileParser.readPrevFileHash(data);
    }
}
