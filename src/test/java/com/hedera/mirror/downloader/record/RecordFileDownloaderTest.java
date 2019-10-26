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

import com.hedera.FileCopier;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.downloader.DownloaderTestingBase;
import com.hedera.mirror.downloader.StreamProperties;
import com.hedera.utilities.Utility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.ResourceUtils;

import java.nio.file.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RecordFileDownloaderTest extends DownloaderTestingBase {
    private RecordFileDownloader downloader;
    private RecordStreamProperties recordStreamProperties;

    @Override
    protected StreamProperties getStreamProperties() {
        this.recordStreamProperties = new RecordStreamProperties(mirrorProperties);
        recordStreamProperties.init();
        return recordStreamProperties;
    }

    @Override
    protected void doDownload() {
        downloader.download();
    }

    @Override
    protected boolean isSigFile(String file) {
        return Utility.isRecordSigFile(file);
    }

    @Override
    protected boolean isDataFile(String file) {
        return Utility.isRecordFile(file);
    }

    @BeforeEach
    void before(TestInfo testInfo) {
        super.beforeEach(testInfo, "recordstreams/v2");
        downloader = new RecordFileDownloader(recordStreamProperties, downloaderFactory);
    }

    @AfterEach
    void after(TestInfo testInfo) {
        super.afterEach(testInfo);
    }

    @Test
    @DisplayName("Download and verify V1 files")
    public void downloadV1() throws Exception {
        Path addressBook = ResourceUtils.getFile("classpath:addressbook/test-v1").toPath();
        mirrorProperties.setAddressBookPath(addressBook);
        fileCopier = FileCopier.create(Utility.getResource("data").toPath(), s3Path)
                .from(recordStreamProperties.getStreamType().getPath(), "v1")
                .to(s3ConnectorProperties.getBucketName(), recordStreamProperties.getStreamType().getPath());
        fileCopier.copy();
        doReturn("").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE);

        downloader.download();

        verify(applicationStatusRepository).updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE, "2019-07-01T14:13:00.317763Z.rcd");
        verify(applicationStatusRepository).updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE, "2019-07-01T14:29:00.302068Z.rcd");
        verify(applicationStatusRepository, times(2)).updateStatusValue(eq(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE_HASH), any());
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(2)
                .allMatch(p -> Utility.isRecordFile(p.toString()))
                .extracting(Path::getFileName)
                .contains(Paths.get("2019-07-01T14:13:00.317763Z.rcd"))
                .contains(Paths.get("2019-07-01T14:29:00.302068Z.rcd"));
    }

    @Test
    @DisplayName("Download and verify V2 files")
    void downloadV2() throws Exception {
        fileCopier.copy();
        doReturn("").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE);
        downloader.download();
        verify(applicationStatusRepository).updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE, "2019-08-30T18_10_00.419072Z.rcd");
        verify(applicationStatusRepository).updateStatusValue(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE, "2019-08-30T18_10_05.249678Z.rcd");
        verify(applicationStatusRepository, times(2)).updateStatusValue(eq(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE_HASH), any());
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(2)
                .allMatch(p -> Utility.isRecordFile(p.toString()))
                .extracting(Path::getFileName)
                .contains(Paths.get("2019-08-30T18_10_05.249678Z.rcd"))
                .contains(Paths.get("2019-08-30T18_10_00.419072Z.rcd"));
    }

    @Test
    @DisplayName("Max download items reached")
    void maxDownloadItemsReached() throws Exception {
        testMaxDownloadItemsReached("2019-08-30T18_10_00.419072Z.rcd");
    }

    @Test
    @DisplayName("Doesn't match last valid hash")
    void hashMismatchWithPrevious() throws Exception {
        final String filename = "2019-08-30T18_10_05.249678Z.rcd";
        doReturn("2019-07-01T14:12:00.000000Z.rcd").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE);
        doReturn("123").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE_HASH);
        doReturn("").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER);
        fileCopier.filterFiles(filename + "*").copy(); // Skip first file with zero hash
        downloader.download();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Bypass previous hash mismatch")
    void hashMismatchWithBypass() throws Exception {
        final String filename = "2019-08-30T18_10_05.249678Z.rcd";
        doReturn("2019-07-01T14:12:00.000000Z.rcd").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE);
        doReturn("123").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE_HASH);
        doReturn("2019-09-01T00:00:00.000000Z.rcd").when(applicationStatusRepository).findByStatusCode(ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER);
        fileCopier.filterFiles(filename + "*").copy(); // Skip first file with zero hash
        downloader.download();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(1)
                .allMatch(p -> Utility.isRecordFile(p.toString()))
                .extracting(Path::getFileName)
                .contains(Paths.get(filename));
    }
}
