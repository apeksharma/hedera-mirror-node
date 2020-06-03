package com.hedera.mirror.importer.parser.record.entity.sql;
/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
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
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Named;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import com.hedera.mirror.importer.domain.ContractResult;
import com.hedera.mirror.importer.domain.CryptoTransfer;
import com.hedera.mirror.importer.domain.EntityId;
import com.hedera.mirror.importer.domain.FileData;
import com.hedera.mirror.importer.domain.LiveHash;
import com.hedera.mirror.importer.domain.NonFeeTransfer;
import com.hedera.mirror.importer.domain.RecordFile;
import com.hedera.mirror.importer.domain.TopicMessage;
import com.hedera.mirror.importer.domain.Transaction;
import com.hedera.mirror.importer.exception.DuplicateFileException;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.exception.ParserException;
import com.hedera.mirror.importer.exception.ParserSQLException;
import com.hedera.mirror.importer.parser.domain.StreamFileData;
import com.hedera.mirror.importer.parser.record.RecordStreamFileListener;
import com.hedera.mirror.importer.parser.record.entity.EntityListener;
import com.hedera.mirror.importer.repository.RecordFileRepository;

@Named
@Log4j2
@RequiredArgsConstructor
public class CopySqlEntityListener implements EntityListener, RecordStreamFileListener {
    static final int HUNDRED_MB = 100 * 1024 * 1024;
    private final SqlProperties properties;
    private final DataSource dataSource;
    private ExecutorService executorService;
    private final RecordFileRepository recordFileRepository;

    private TopicMessageCopyWriter topicMessageCopyWriter;
    private TransactionsCopyWriter transactionsCopyWriter;
    private CryptoTransferCopyWriter cryptoTransferCopyWriter;

    @Override
    public void onStart(StreamFileData streamFileData) {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(properties.getExecutorSize());
        }
        String fileName = streamFileData.getFilename();
        if (recordFileRepository.findByName(fileName).size() > 0) {
            throw new DuplicateFileException("File already exists in the database: " + fileName);
        }
        try {
            topicMessageCopyWriter = new TopicMessageCopyWriter(dataSource, properties.getBatchSize(), executorService);
            transactionsCopyWriter = new TransactionsCopyWriter(dataSource, properties.getBatchSize(), executorService);
            cryptoTransferCopyWriter = new CryptoTransferCopyWriter(
                    dataSource, properties.getBatchSize(), executorService);
        } catch (Exception e) {
            throw new ParserException("Error setting up connection and statements", e);
        }
    }

    @Override
    public void onEnd(RecordFile recordFile) {
        try {
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(() -> transactionsCopyWriter.flush()),
                    CompletableFuture.runAsync(() -> cryptoTransferCopyWriter.flush()),
                    CompletableFuture.runAsync(() -> topicMessageCopyWriter.flush())
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            throw new ParserException(e);
        }
        recordFileRepository.save(recordFile);
    }

    @Override
    public void onError() {
    }

    @Override
    public void onTransaction(Transaction transaction) throws ImporterException {
        transactionsCopyWriter.onEntity(transaction);
    }

    @Override
    public void onCryptoTransfer(CryptoTransfer cryptoTransfer) throws ImporterException {
        cryptoTransferCopyWriter.onEntity(cryptoTransfer);
    }

    @Override
    public void onTopicMessage(TopicMessage topicMessage) throws ImporterException {
        topicMessageCopyWriter.onEntity(topicMessage);
    }

    @Override
    public void onEntityId(EntityId entityId) throws ImporterException {
        // TODO
    }

    @Override
    public void onNonFeeTransfer(NonFeeTransfer nonFeeTransfer) throws ImporterException {
        // TODO
    }

    @Override
    public void onContractResult(ContractResult contractResult) throws ImporterException {
        // TODO
    }

    @Override
    public void onLiveHash(LiveHash liveHash) throws ImporterException {
        // TODO
    }

    @Override
    public void onFileData(FileData fileData) {
        // TODO
    }

    @RequiredArgsConstructor
    private abstract static class CopyWriter<T> {
        protected final Logger log = LogManager.getLogger(getClass());

        final AtomicLong csvTime = new AtomicLong(0L);
        final AtomicLong totalTime = new AtomicLong(0L);
        private final DataSource dataSource;
        private final int batchSize;
        private final Executor executor;
        private final List<T> entities = new ArrayList<>();

        public void onEntity(T entity) {
            entities.add(entity);
        }

        public abstract String getTableDescriptor();
        public abstract void writeCsv(CSVPrinter csvPrinter, T entity);

        public void flush() {
            try {
                ArrayList<CompletableFuture> futures = new ArrayList<>();
                int startIndex = 0;
                while (true) {
                    int endIndex = Math.min(startIndex + batchSize, entities.size());
                    List<T> subList = entities.subList(startIndex, endIndex);
                    futures.add(CompletableFuture.runAsync(() ->  writeEntities(subList), executor));
                    if (endIndex == entities.size()) {
                        break;
                    }
                    startIndex = endIndex;
                }
                for (var future : futures) {
                    future.get();
                }
                log.info("csvTime = {}, totalTime = {}", csvTime.get(), totalTime.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error(e);
                throw new RuntimeException(e);
            }
        }

        public void writeEntities(List<T> entities) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            Connection connection;
            try {
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                log.error("Error getting connection ", e);
                throw new ParserSQLException(e);
            }
            try {
                StringBuilder stringBuilder = new StringBuilder(HUNDRED_MB);
                CSVPrinter csvPrinter = new CSVPrinter(stringBuilder, CSVFormat.DEFAULT);
                for (T entity : entities) {
                    writeCsv(csvPrinter, entity);
                }
                String csvData = stringBuilder.toString();
                CopyManager copyManager = connection.unwrap(PgConnection.class).getCopyAPI();
                csvTime.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                long rowsCount = copyManager.copyIn(
                        "COPY " + getTableDescriptor() + " FROM STDIN WITH CSV ENCODING 'UTF8'",
                        new StringReader(csvData), HUNDRED_MB);
                totalTime.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                log.info("Inserted {} rows in {}ms. Csv length = {}",
                        rowsCount, stopwatch.elapsed(TimeUnit.MILLISECONDS), csvData.length());
            } catch (IOException | SQLException e) {
                log.error(e);
                throw new ParserException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("Exception closing connection", e);
                }
            }
        }
    }

    private static class TransactionsCopyWriter extends CopyWriter<Transaction> {
        public TransactionsCopyWriter(DataSource dataSource, int batchSize, Executor executor) {
            super(dataSource, batchSize, executor);
        }

        @Override
        public String getTableDescriptor() {
            return "t_transactions(consensus_ns, type, result, payer_account_id, valid_start_ns, valid_duration_seconds," +
                    "node_account_id, entity_id, initial_balance, max_fee, charged_tx_fee, memo, " +
                    "transaction_hash, transaction_bytes)";
        }

        @Override
        public void writeCsv(CSVPrinter csvPrinter, Transaction tx) {
            try {
                var entity = tx.getEntityId();

                csvPrinter.printRecord(
                        tx.getConsensusNs(), tx.getType(), tx.getResult(), tx.getPayerAccountId().getId(),
                        tx.getValidStartNs(), tx.getValidDurationSeconds(), tx.getNodeAccountId().getId(),
                        entity == null ?  null : entity.getId(), tx.getInitialBalance(), tx.getMaxFee(), tx.getChargedTxFee(),
                        tx.getMemo(), tx.getTransactionHash(), tx.getTransactionBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.trace("added transaction at timestamp {}", tx.getConsensusNs());
        }
    }

    private static class CryptoTransferCopyWriter extends CopyWriter<CryptoTransfer> {
        public CryptoTransferCopyWriter(DataSource dataSource, int batchSize, Executor executor) {
            super(dataSource, batchSize, executor);
        }

        @Override
        public String getTableDescriptor() {
            return "crypto_transfer(entity_id, consensus_timestamp, amount)";
        }

        @Override
        public void writeCsv(CSVPrinter csvPrinter, CryptoTransfer cryptoTransfer) {
            try {
                csvPrinter.printRecord(cryptoTransfer.getEntityId(), cryptoTransfer.getConsensusTimestamp(), cryptoTransfer.getAmount());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.trace("added crypto transfer for entity_num = {}", cryptoTransfer.getEntityId());
        }
    }

    private static class TopicMessageCopyWriter extends CopyWriter<TopicMessage> {
        public TopicMessageCopyWriter(DataSource dataSource, int batchSize, Executor executor) {
            super(dataSource, batchSize, executor);
        }

        @Override
        public String getTableDescriptor() {
            return "topic_message(consensus_timestamp, realm_num, topic_num, message, running_hash, sequence_number, " +
                    "running_hash_version)";
        }

        @Override
        public void writeCsv(CSVPrinter csvPrinter, TopicMessage topicMessage) {
            try {
                csvPrinter.printRecord(topicMessage.getConsensusTimestamp(), topicMessage.getRealmNum(),
                        topicMessage.getTopicNum(), toHex(topicMessage.getMessage()),
                        toHex(topicMessage.getRunningHash()), topicMessage.getSequenceNumber(),
                        topicMessage.getRunningHashVersion());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.trace("added topic message for entity_num = {}", topicMessage.getTopicNum());
        }
    }

    private static String toHex(byte[] data) {
        if (data == null) {
            return null;
        } else {
            return "\\x" + Hex.encodeHexString(data);
        }
    }
}
