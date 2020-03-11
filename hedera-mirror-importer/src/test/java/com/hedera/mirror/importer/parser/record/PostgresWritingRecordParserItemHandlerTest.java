package com.hedera.mirror.importer.parser.record;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2020 Hedera Hashgraph, LLC
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Resource;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

import com.hedera.mirror.importer.IntegrationTest;
import com.hedera.mirror.importer.domain.ContractResult;
import com.hedera.mirror.importer.domain.CryptoTransfer;
import com.hedera.mirror.importer.domain.FileData;
import com.hedera.mirror.importer.domain.LiveHash;
import com.hedera.mirror.importer.domain.NonFeeTransfer;
import com.hedera.mirror.importer.domain.RecordFile;
import com.hedera.mirror.importer.domain.TopicMessage;
import com.hedera.mirror.importer.domain.Transaction;
import com.hedera.mirror.importer.parser.domain.StreamFileData;
import com.hedera.mirror.importer.repository.ContractResultRepository;
import com.hedera.mirror.importer.repository.CryptoTransferRepository;
import com.hedera.mirror.importer.repository.FileDataRepository;
import com.hedera.mirror.importer.repository.LiveHashRepository;
import com.hedera.mirror.importer.repository.NonFeeTransferRepository;
import com.hedera.mirror.importer.repository.TopicMessageRepository;
import com.hedera.mirror.importer.repository.TransactionRepository;

@Sql(executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD, scripts = "classpath:db/scripts/cleanup.sql")
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = "classpath:db/scripts/cleanup.sql")
public class PostgresWritingRecordParserItemHandlerTest extends IntegrationTest {

    @Resource
    protected TransactionRepository transactionRepository;

    @Resource
    protected CryptoTransferRepository cryptoTransferRepository;

    @Resource
    protected NonFeeTransferRepository nonFeeTransferRepository;

    @Resource
    protected ContractResultRepository contractResultRepository;

    @Resource
    protected LiveHashRepository liveHashRepository;

    @Resource
    protected FileDataRepository fileDataRepository;

    @Resource
    protected TopicMessageRepository topicMessageRepository;

    @Resource
    protected PostgresWritingRecordParsedItemHandler postgresWriter;

    @Resource
    protected PostgresWriterProperties postgresWriterProperties;

    private RecordFile recordFile;

    private static final String FILE_NAME = "fileName";

    @BeforeEach
    final void beforeEach() {
        recordFile = postgresWriter.onStart(new StreamFileData(FILE_NAME, null)).get();
    }

    void completeFileAndCommit() {
        postgresWriter.onEnd(recordFile);
    }

    @Test
    void onCryptoTransferList() throws Exception {
        // given
        CryptoTransfer cryptoTransfer1 = new CryptoTransfer(1L, 1L, 0L, 1L);
        CryptoTransfer cryptoTransfer2 = new CryptoTransfer(2L, -2L, 0L, 2L);

        // when
        postgresWriter.onCryptoTransfer(cryptoTransfer1);
        postgresWriter.onCryptoTransfer(cryptoTransfer2);
        completeFileAndCommit();

        // then
        assertEquals(2, cryptoTransferRepository.count());
        assertExistsAndEquals(cryptoTransferRepository, cryptoTransfer1, 1L);
        assertExistsAndEquals(cryptoTransferRepository, cryptoTransfer2, 2L);
    }

    @Test
    void onNonFeeTransfer() throws Exception {
        // given
        NonFeeTransfer nonFeeTransfer1 = new NonFeeTransfer(1L, 1L, 0L, 1L);
        NonFeeTransfer nonFeeTransfer2 = new NonFeeTransfer(2L, -2L, 0L, 2L);

        // when
        postgresWriter.onNonFeeTransfer(nonFeeTransfer1);
        postgresWriter.onNonFeeTransfer(nonFeeTransfer2);
        completeFileAndCommit();

        // then
        assertEquals(2, nonFeeTransferRepository.count());
        assertExistsAndEquals(nonFeeTransferRepository, nonFeeTransfer1, 1L);
        assertExistsAndEquals(nonFeeTransferRepository, nonFeeTransfer2, 2L);
    }

    @Test
    void onTopicMessage() throws Exception {
        // given
        byte[] message = Strings.toByteArray("test message");
        byte[] runningHash = Strings.toByteArray("running hash");
        TopicMessage expectedTopicMessage = new TopicMessage(1L, message, 0, runningHash, 10L, 1001);

        // when
        postgresWriter.onTopicMessage(expectedTopicMessage);
        completeFileAndCommit();

        // then
        assertEquals(1, topicMessageRepository.count());
        assertExistsAndEquals(topicMessageRepository, expectedTopicMessage, 1L);
    }

    @Test
    void onFileData() throws Exception {
        // given
        FileData expectedFileData = new FileData(11L, Strings.toByteArray("file data"));

        // when
        postgresWriter.onFileData(expectedFileData);
        completeFileAndCommit();

        // then
        assertEquals(1, fileDataRepository.count());
        assertExistsAndEquals(fileDataRepository, expectedFileData, 11L);
    }

    @Test
    void onContractResult() throws Exception {
        // given
        ContractResult expectedContractResult = new ContractResult(15L, Strings.toByteArray("function parameters"),
                10000L, Strings.toByteArray("call result"), 10000L);

        // when
        postgresWriter.onContractResult(expectedContractResult);
        completeFileAndCommit();

        // then
        assertEquals(1, contractResultRepository.count());
        assertExistsAndEquals(contractResultRepository, expectedContractResult, 15L);
    }

    @Test
    void onLiveHash() throws Exception {
        // given
        LiveHash expectedLiveHash = new LiveHash(20L, Strings.toByteArray("live hash"));

        // when
        postgresWriter.onLiveHash(expectedLiveHash);
        completeFileAndCommit();

        // then
        assertEquals(1, liveHashRepository.count());
        assertExistsAndEquals(liveHashRepository, expectedLiveHash, 20L);
    }

    @Test
    void onTransaction() throws Exception {
        // given
        Transaction expectedTransaction = new Transaction(101L, 0L, Strings.toByteArray("memo"), 0, 0, 1L, 1L, 1L, null,
                0L, 1L, 1L, 1L, Strings.toByteArray("transactionHash"), null);

        // when
        postgresWriter.onTransaction(expectedTransaction);
        completeFileAndCommit();

        // then
        assertEquals(1, transactionRepository.count());
        assertExistsAndEquals(transactionRepository, expectedTransaction, 101L);
    }

    // Test that on seeing 'batchSize' number of transactions, 'executeBatch()' is called for all PreparedStatements
    // issued by the connection.
    @Test
    void batchSize() throws Exception {
        // given
        int batchSize = 10;
        postgresWriterProperties.setBatchSize(batchSize);

        CallableStatement fileCreate = mock(CallableStatement.class);
        when(fileCreate.getLong(any(Integer.class))).thenAnswer(ignore -> 1L);

        Connection connection = mock(Connection.class);
        when(connection.prepareCall(any())).thenReturn(fileCreate);
        List<PreparedStatement> insertStatements = new ArrayList<>(); // tracks all PreparedStatements
        when(connection.prepareStatement(any())).then(ignored -> {
            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            when(preparedStatement.executeBatch()).thenReturn(new int[] {});
            insertStatements.add(preparedStatement);
            return preparedStatement;
        });

        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);
        PostgresWritingRecordParsedItemHandler postgresWriter2 =
                new PostgresWritingRecordParsedItemHandler(postgresWriterProperties, dataSource);
        RecordFile recordFile = postgresWriter2.onStart(new StreamFileData("fileName", null)).get();

        // when
        for (int i = 0; i < batchSize; i++) {
            postgresWriter2.onTransaction(mock(Transaction.class));
        }

        // then
        for (PreparedStatement ps : insertStatements) {
            verify(ps).executeBatch();
        }

        postgresWriter2.onEnd(recordFile); // close connections
        completeFileAndCommit();  // close postgresWriter
    }

    @Test
    void onError() {
        // when
        postgresWriter.onNonFeeTransfer(new NonFeeTransfer(1L, 1L, 0L, 1L));
        postgresWriter.onCryptoTransfer(new CryptoTransfer(2L, -2L, 0L, 2L));
        postgresWriter.onError();

        // then
        assertEquals(0, nonFeeTransferRepository.count());
        assertEquals(0, cryptoTransferRepository.count());
    }

    @Test
    void onDuplicateFileReturnEmpty() {
        // given: file processed once
        completeFileAndCommit();

        // when
        var recordFile = postgresWriter.onStart(new StreamFileData(FILE_NAME, null));

        // then
        assertTrue(recordFile.isEmpty());

        // Since no onEnd/onError would be called, SQL resource should have been released already. No explicit
        // connection cleanup here.
    }

    static <T, ID> void assertExistsAndEquals(CrudRepository<T, ID> repository, T expected, ID id) throws Exception {
        Optional<T> actual = repository.findById(id);
        assertTrue(actual.isPresent());
        assertEquals(expected, actual.get());
    }
}
