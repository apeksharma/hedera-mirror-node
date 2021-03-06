package com.hedera.parser;

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

import com.google.common.collect.Sets;
import com.hedera.FileCopier;
import com.hedera.IntegrationTest;
import com.hedera.configLoader.ConfigLoader;
import com.hedera.mirror.config.RecordProperties;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.Transaction;
import com.hedera.mirror.repository.ApplicationStatusRepository;
import com.hedera.mirror.repository.TransactionRepository;
import com.hedera.utilities.Utility;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.jdbc.Sql;

import javax.annotation.Resource;
import java.io.File;
import java.nio.file.*;

import static org.assertj.core.api.Assertions.assertThat;

@Sql("classpath:db/scripts/cleanup.sql") // Class manually commits so have to manually cleanup tables
public class RecordFileParserTest extends IntegrationTest {

    @Resource
    private RecordFileParser recordFileParser;

    @Resource
    private ApplicationStatusRepository applicationStatusRepository;

    @Resource
    private TransactionRepository transactionRepository;

    @Resource
    private RecordProperties recordProperties;

    @TempDir
    Path dataPath;

    @Value("classpath:data")
    Path testPath;

    private Path parsedPath;
    private FileCopier fileCopier;

    @BeforeEach
    void before() {
        ConfigLoader.setDownloadToDir(dataPath.toAbsolutePath().toString());
        fileCopier = FileCopier.create(testPath, dataPath)
                .from("recordstreams", "v2", "record0.0.3")
                .filterFiles("*.rcd")
                .to("recordstreams", "valid");
        parsedPath = dataPath.resolve("recordstreams").resolve("parsedRecordFiles");
        Utility.ensureDirectory(parsedPath.toString());
    }

    @Test
    void parse() throws Exception {
        fileCopier.copy();
        recordFileParser.parse();

        assertThat(Files.walk(parsedPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(2)
                .extracting(Path::getFileName)
                .contains(Paths.get("2019-08-30T18_10_05.249678Z.rcd"))
                .contains(Paths.get("2019-08-30T18_10_00.419072Z.rcd"));

        assertThat(transactionRepository.findAll())
                .hasSize(19 + 15)
                .extracting(Transaction::getTransactionTypeId)
                .containsOnlyElementsOf(Sets.newHashSet(2, 4, 14));
    }

    @Test
    void disabled() throws Exception {
        recordProperties.setEnabled(false);
        fileCopier.copy();
        recordFileParser.parse();
        assertThat(Files.walk(parsedPath)).filteredOn(p -> !p.toFile().isDirectory()).hasSize(0);
        assertThat(transactionRepository.count()).isEqualTo(0L);
    }

    @Test
    void noFiles() throws Exception {
        recordFileParser.parse();
        assertThat(Files.walk(parsedPath)).filteredOn(p -> !p.toFile().isDirectory()).hasSize(0);
        assertThat(transactionRepository.count()).isEqualTo(0L);
    }

    @Test
    void invalidFile() throws Exception {
        File recordFile = dataPath.resolve("recordstreams").resolve("valid").resolve("2019-08-30T18_10_05.249678Z.rcd").toFile();
        FileUtils.writeStringToFile(recordFile, "corrupt", "UTF-8");
        recordFileParser.parse();
        assertThat(Files.walk(parsedPath)).filteredOn(p -> !p.toFile().isDirectory()).hasSize(0);
        assertThat(transactionRepository.count()).isEqualTo(0L);
    }

    @Test
    void hashMismatch() throws Exception {
        applicationStatusRepository.updateStatusValue(ApplicationStatusCode.LAST_PROCESSED_RECORD_HASH, "123");
        fileCopier.copy();
        recordFileParser.parse();
        assertThat(Files.walk(parsedPath)).filteredOn(p -> !p.toFile().isDirectory()).hasSize(0);
        assertThat(transactionRepository.count()).isEqualTo(0L);
    }

    @Test
    void bypassHashMismatch() throws Exception {
        applicationStatusRepository.updateStatusValue(ApplicationStatusCode.LAST_PROCESSED_RECORD_HASH, "123");
        applicationStatusRepository.updateStatusValue(ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER, "2019-09-01T00:00:00.000000Z.rcd");
        fileCopier.copy();
        recordFileParser.parse();

        assertThat(Files.walk(parsedPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(2)
                .extracting(Path::getFileName)
                .contains(Paths.get("2019-08-30T18_10_05.249678Z.rcd"))
                .contains(Paths.get("2019-08-30T18_10_00.419072Z.rcd"));

        assertThat(transactionRepository.findAll()).hasSize(19 + 15);
    }
}
