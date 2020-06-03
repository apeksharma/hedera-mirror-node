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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.repository.CrudRepository;

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
import com.hedera.mirror.importer.parser.domain.StreamFileData;
import com.hedera.mirror.importer.parser.record.RecordStreamFileListener;
import com.hedera.mirror.importer.parser.record.entity.ConditionOnEntityRecordParser;
import com.hedera.mirror.importer.parser.record.entity.EntityListener;
import com.hedera.mirror.importer.repository.CryptoTransferRepository;
import com.hedera.mirror.importer.repository.RecordFileRepository;
import com.hedera.mirror.importer.repository.TopicMessageRepository;
import com.hedera.mirror.importer.repository.TransactionRepository;

@Log4j2
//@Named
@RequiredArgsConstructor
@ConditionOnEntityRecordParser
public class RepositorySqlEntityListener implements EntityListener, RecordStreamFileListener {
    private final SqlProperties properties;
    private final RecordFileRepository recordFileRepository;
    private final TransactionRepository transactionRepository;
    private final CryptoTransferRepository cryptoTransferRepository;
//    private final NonFeeTransferRepository nonFeeTransferRepository;
//    private final FileDataRepository fileDataRepository;
//    private final ContractResultRepository contractResultRepository;
//    private final LiveHashRepository liveHashRepository;
    private final TopicMessageRepository topicMessageRepository;
    private ExecutorService executorService;

    private List<Transaction> transactions;
    private List<CryptoTransfer> cryptoTransfers;
//    private List<NonFeeTransfer> nonFeeTransfers;
//    private List<FileData> fileData;
//    private List<ContractResult> contractResults;
//    private List<LiveHash> liveHashes;
    private List<TopicMessage> topicMessages;

    @Override
    public void onStart(StreamFileData streamFileData) {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(properties.getExecutorSize());
        }
        String fileName = streamFileData.getFilename();
        if (recordFileRepository.findByName(fileName).size() > 0) {
            throw new DuplicateFileException("File already exists in the database: " + fileName);
        }
        transactions = new ArrayList<>();
        cryptoTransfers = new ArrayList<>();
//        nonFeeTransfers = new ArrayList<>();
//        fileData = new ArrayList<>();
//        contractResults = new ArrayList<>();
//        liveHashes = new ArrayList<>();
        topicMessages = new ArrayList<>();
    }

    @Override
    public void onEnd(RecordFile recordFile) {
        executeBatches();
//        try {
            recordFileRepository.save(recordFile);
            // commit the changes to the database
//        } catch (SQLException e) {
//            throw new ParserSQLException(e);
//        }
    }

    @Override
    public void onError() {
//        try {
//            closeConnectionAndStatements();
//        } catch (SQLException e) {
//            log.error("Exception while rolling transaction back", e);
//        }
    }

    private void executeBatches() {
        // Approach 2: Use batch size = 1_000_000
        // Approach 3: use batch size = 10_000
//        try {
//            CompletableFuture.allOf(
//                    CompletableFuture.runAsync(new BatchInsert(
//                            transactionRepository, transactions, properties.getBatchSize(), "transactions", executorService)),
//                    CompletableFuture.runAsync(new BatchInsert(
//                            cryptoTransferRepository, cryptoTransfers, properties.getBatchSize(), "crypto transfers", executorService)),
//                    CompletableFuture.runAsync(new BatchInsert(
//                            topicMessageRepository, topicMessages, properties.getBatchSize(), "topic messages", executorService)))
//                    .get();
//        } catch (InterruptedException | ExecutionException e) {
//            log.error(e);
//            throw new RuntimeException(e);
//        }

        // Approach 1:
        transactionRepository.saveAll(transactions);
        topicMessageRepository.saveAll(topicMessages);
        cryptoTransferRepository.saveAll(cryptoTransfers);
    }

    @RequiredArgsConstructor
    static public class BatchInsert<T, ID> implements Runnable {
        private final CrudRepository<T, ID> repository;
        private final List<T> entities;
        private final int batchSize;
        private final String type;
        private final Executor executor;

        @Override
        public void run() {
            try {
                ArrayList<CompletableFuture> futures = new ArrayList<>();
                int startIndex = 0;
                while (true) {
                    int endIndex = Math.min(startIndex + batchSize, entities.size());
                    log.info("Starting to insert {} from {} to {}", type, startIndex, endIndex);
                    List<T> subList = entities.subList(startIndex, endIndex);
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            Stopwatch stopwatch = Stopwatch.createStarted();
                            repository.saveAll(subList);
                            log.info("Inserted {} {} in {}ms", subList.size(), type, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        } catch (Exception e) {
                            log.error(e);
                            throw new RuntimeException(e);
                        }
                   }, executor));
                    if (endIndex == entities.size()) {
                        break;
                    }
                    startIndex = endIndex;
                }
                for (var future : futures) {
                    future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onTransaction(Transaction transaction) throws ImporterException {
        transactions.add(transaction);
    }

    @Override
    public void onCryptoTransfer(CryptoTransfer cryptoTransfer) throws ImporterException {
        cryptoTransfers.add(cryptoTransfer);
    }

    @Override
    public void onNonFeeTransfer(NonFeeTransfer nonFeeTransfer) throws ImporterException {
    }

    @Override
    public void onTopicMessage(TopicMessage topicMessage) throws ImporterException {
        topicMessages.add(topicMessage);
    }

    @Override
    public void onEntityId(EntityId entityId) throws ImporterException {
    }

    @Override
    public void onContractResult(ContractResult contractResult) throws ImporterException {
    }

    @Override
    public void onFileData(FileData fileData) throws ImporterException {
    }

    @Override
    public void onLiveHash(LiveHash liveHash) throws ImporterException {
    }
}
