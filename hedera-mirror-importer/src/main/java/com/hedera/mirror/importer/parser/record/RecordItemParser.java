package com.hedera.mirror.importer.parser.record;

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

import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ContractCallTransactionBody;
import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoAddClaimTransactionBody;
import com.hederahashgraph.api.proto.java.FileAppendTransactionBody;
import com.hederahashgraph.api.proto.java.FileID;
import com.hederahashgraph.api.proto.java.FileUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.hederahashgraph.api.proto.java.TransferList;
import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;

import com.hedera.mirror.importer.addressbook.NetworkAddressBook;
import com.hedera.mirror.importer.domain.ContractResult;
import com.hedera.mirror.importer.domain.CryptoTransfer;
import com.hedera.mirror.importer.domain.Entities;
import com.hedera.mirror.importer.domain.EntityId;
import com.hedera.mirror.importer.domain.EntityType;
import com.hedera.mirror.importer.domain.FileData;
import com.hedera.mirror.importer.domain.LiveHash;
import com.hedera.mirror.importer.domain.NonFeeTransfer;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.exception.ParserException;
import com.hedera.mirror.importer.parser.CommonParserProperties;
import com.hedera.mirror.importer.parser.domain.RecordItem;
import com.hedera.mirror.importer.parser.record.transactionhandler.EntityHelper;
import com.hedera.mirror.importer.parser.record.transactionhandler.TransactionHandler;
import com.hedera.mirror.importer.parser.record.transactionhandler.TransactionHandlerFactory;
import com.hedera.mirror.importer.repository.EntityRepository;
import com.hedera.mirror.importer.repository.EntityTypeRepository;
import com.hedera.mirror.importer.util.Utility;

@Log4j2
@Named
public class RecordItemParser implements RecordItemListener {
    private final RecordParserProperties parserProperties;
    private final NetworkAddressBook networkAddressBook;
    private final EntityRepository entityRepository;
    private final EntityTypeRepository entityTypeRepository;
    private final NonFeeTransferExtractionStrategy nonFeeTransfersExtractor;
    private final Predicate<com.hedera.mirror.importer.domain.Transaction> transactionFilter;
    private final RecordParsedItemHandler recordParsedItemHandler;
    private final TransactionHandlerFactory transactionHandlerFactory;
    private final EntityHelper entityHelper;

    public RecordItemParser(CommonParserProperties commonParserProperties, RecordParserProperties parserProperties,
                            NetworkAddressBook networkAddressBook, EntityRepository entityRepository,
                            EntityTypeRepository entityTypeRepository,
                            NonFeeTransferExtractionStrategy nonFeeTransfersExtractor,
                            RecordParsedItemHandler recordParsedItemHandler,
                            TransactionHandlerFactory transactionHandlerFactory,
                            EntityHelper entityHelper) {
        this.parserProperties = parserProperties;
        this.networkAddressBook = networkAddressBook;
        this.entityRepository = entityRepository;
        this.entityTypeRepository = entityTypeRepository;
        this.nonFeeTransfersExtractor = nonFeeTransfersExtractor;
        this.recordParsedItemHandler = recordParsedItemHandler;
        this.transactionHandlerFactory = transactionHandlerFactory;
        this.entityHelper = entityHelper;
        transactionFilter = commonParserProperties.getFilter();
    }

    public static boolean isSuccessful(TransactionRecord transactionRecord) {
        return ResponseCodeEnum.SUCCESS == transactionRecord.getReceipt().getStatus();
    }

    private static boolean isFileAddressBook(FileID fileId) {
        return (fileId.getFileNum() == 102) && (fileId.getShardNum() == 0) && (fileId.getRealmNum() == 0);
    }

    /**
     * Because body.getDataCase() can return null for unknown transaction types, we instead get oneof generically
     *
     * @param body
     * @return The protobuf ID that represents the transaction type
     */
    private static int getTransactionType(TransactionBody body) {
        TransactionBody.DataCase dataCase = body.getDataCase();

        if (dataCase == null || dataCase == TransactionBody.DataCase.DATA_NOT_SET) {
            Set<Integer> unknownFields = body.getUnknownFields().asMap().keySet();

            if (unknownFields.size() != 1) {
                throw new IllegalStateException("Unable to guess correct transaction type since there's not exactly " +
                        "one: " + unknownFields);
            }

            int transactionType = unknownFields.iterator().next();
            log.warn("Encountered unknown transaction type: {}", transactionType);
            return transactionType;
        }

        return dataCase.getNumber();
    }

    @Override
    public void onItem(RecordItem recordItem) throws ImporterException {
        TransactionBody body = recordItem.getTransactionBody();
        TransactionRecord txRecord = recordItem.getRecord();
        TransactionHandler transactionHandler = transactionHandlerFactory.create(body);
        log.trace("Storing transaction body: {}", () -> Utility.printProtoMessage(body));

        Entities entity = null; // Entity used when t_entities row must be updated.
        EntityId entityId = null; // Entity ID simply used for reference purposes (in the transaction object)

        /**
         * If the transaction wasn't successful don't update the entity.
         * Still include the transfer list.
         * Still create the entity (empty) and reference it from t_transactions, as it would have been validated
         * to exist in preconsensus checks.
         * Don't update any attributes of the entity.
         */
        boolean doUpdateEntity = isSuccessful(txRecord);

        // TODO
        //   entityId = getEntityId()
        //   if (updatesEntity && doUpdateEntity && entityId != null) {
        //     fetch Entities from repo
        //     call updateEntity(...) // temporarily, it's okay if updateEntity is doing repo IO too
        //   } else {
        //     fetch entityId.id
        //   }

        long validDurationSeconds = body.hasTransactionValidDuration() ?
                body.getTransactionValidDuration().getSeconds() : null;
        long consensusNs = Utility.timeStampInNanos(txRecord.getConsensusTimestamp());

        com.hedera.mirror.importer.domain.Transaction tx = new com.hedera.mirror.importer.domain.Transaction();
        tx.setChargedTxFee(txRecord.getTransactionFee());
        tx.setConsensusNs(consensusNs);
        // If entityId is non-null, then 'entity' is null.
        // Entity cannot be saved to repo yet since the filter below may want to ignore the transaction.
        // Database lookups are done after the filtering, so entity.id is set later.
        if (entityId != null) {
            entity = new Entities();
            entity.setEntityShard(entityId.getEntityShard());
            entity.setEntityRealm(entityId.getEntityRealm());
            entity.setEntityNum(entityId.getEntityNum());
            entity.setEntityTypeId(entityId.getEntityTypeId());
        }
        tx.setEntity(entity);
        tx.setInitialBalance(initialBalance);
        tx.setMemo(body.getMemo().getBytes());
        tx.setMaxFee(body.getTransactionFee());
        tx.setResult(txRecord.getReceipt().getStatusValue());
        tx.setType(getTransactionType(body));
        tx.setTransactionBytes(parserProperties.getPersist().isTransactionBytes() ? recordItem
                .getTransactionBytes() : null);
        tx.setTransactionHash(txRecord.getTransactionHash().toByteArray());
        tx.setValidDurationSeconds(validDurationSeconds);
        tx.setValidStartNs(Utility.timeStampInNanos(body.getTransactionID().getTransactionValidStart()));

        if (!transactionFilter.test(tx)) {
            log.debug("Ignoring transaction {}", tx);
            return;
        }

        // th.updateTransaction(tx, ri)

        // If transaction handler never updates the entity, it means 'entity' was null and was set based on 'entityId'.
        // We need to lookup/create id for the entity and set 'entity.id'.
        if (transactionHandler.updatesEntity()) {
            entity.setId(entityHelper.lookupOrCreateId(entityId));
        } else {
            // save the entity since it may have been updated
            EntityId proxyEntityId = transactionHandler.getProxyEntityId(recordItem);
            if (proxyEntityId != null) {
                entity.setProxyAccountId(entityHelper.lookupOrCreateId(proxyEntityId));
            }
            if (entity.getAutoRenewAccount() != null) {
                // TODO: lookupOrCreate id.
                entity.setAutoRenewAccount(createEntity(entity.getAutoRenewAccount()));
            }
            entityRepository.save(entity);
        }

        AccountID payerAccountId = body.getTransactionID().getAccountID();
        EntityId payerEntityId = getEntityId(payerAccountId);
        EntityId nodeEntityId = getEntityId(body.getNodeAccountID());
        tx.setNodeAccountId(nodeEntityId.getId());
        tx.setPayerAccountId(payerEntityId.getId());

        if ((txRecord.hasTransferList()) && parserProperties.getPersist().isCryptoTransferAmounts()) {
            processNonFeeTransfers(consensusNs, payerAccountId, body, txRecord);
            if (body.hasCryptoCreateAccount() && isSuccessful(txRecord)) {
                insertCryptoCreateTransferList(consensusNs, txRecord, body, txRecord.getReceipt()
                        .getAccountID(), payerAccountId);
            } else {
                insertTransferList(consensusNs, txRecord.getTransferList());
            }
        }

        // TransactionBody-specific handlers.
        if (body.hasContractCall()) {
            insertContractCall(consensusNs, body.getContractCall(), txRecord);
        } else if (body.hasContractCreateInstance()) {
            insertContractCreateInstance(consensusNs, body.getContractCreateInstance(), txRecord);
        }
        if (doUpdateEntity) {
            if (body.hasConsensusSubmitMessage()) {
                insertConsensusTopicMessage(body.getConsensusSubmitMessage(), txRecord);
            } else if (body.hasCryptoAddClaim()) {
                insertCryptoAddClaim(consensusNs, body.getCryptoAddClaim());
            } else if (body.hasFileAppend()) {
                insertFileAppend(consensusNs, body.getFileAppend());
            } else if (body.hasFileCreate()) {
                insertFileData(consensusNs, body.getFileCreate().getContents().toByteArray(),
                        txRecord.getReceipt().getFileID());
            } else if (body.hasFileUpdate()) {
                insertFileUpdate(consensusNs, body.getFileUpdate());
            }
        }

        recordParsedItemHandler.onTransaction(tx);
        log.debug("Storing transaction: {}", tx);
    }

    /**
     * Should the given transaction/record generate non_fee_transfers based on what type the transaction is, it's
     * status, and run-time configuration concerning which situations warrant storing.
     *
     * @param body
     * @param transactionRecord
     * @return
     */
    private boolean shouldStoreNonFeeTransfers(TransactionBody body, TransactionRecord transactionRecord) {
        if (!body.hasCryptoCreateAccount() && !body.hasContractCreateInstance() && !body.hasCryptoTransfer() && !body
                .hasContractCall()) {
            return false;
        }
        return parserProperties.getPersist().isNonFeeTransfers();
    }

    /**
     * Additionally store rows in the non_fee_transactions table if applicable. This will allow the rest-api to create
     * an itemized set of transfers that reflects non-fees (explicit transfers), threshold records, node fee, and
     * network+service fee (paid to treasury).
     */
    private void processNonFeeTransfers(long consensusTimestamp, AccountID payerAccountId,
                                        TransactionBody body, TransactionRecord transactionRecord) {
        if (!shouldStoreNonFeeTransfers(body, transactionRecord)) {
            return;
        }

        for (var aa : nonFeeTransfersExtractor.extractNonFeeTransfers(payerAccountId, body, transactionRecord)) {
            addNonFeeTransferInserts(consensusTimestamp, aa.getAccountID().getRealmNum(),
                    aa.getAccountID().getAccountNum(), aa.getAmount());
        }
    }

    private void addNonFeeTransferInserts(long consensusTimestamp, long realm, long accountNum, long amount) {
        if (0 != amount) {
            recordParsedItemHandler.onNonFeeTransfer(
                    new NonFeeTransfer(consensusTimestamp, realm, accountNum, amount));
        }
    }

    private void insertFileData(long consensusTimestamp, byte[] contents, FileID fileID) {
        if (parserProperties.getPersist().isFiles() ||
                (parserProperties.getPersist().isSystemFiles() && fileID.getFileNum() < 1000)) {
            recordParsedItemHandler.onFileData(new FileData(consensusTimestamp, contents));
        }
    }

    private void insertFileAppend(long consensusTimestamp, FileAppendTransactionBody transactionBody) {
        byte[] contents = transactionBody.getContents().toByteArray();
        insertFileData(consensusTimestamp, contents, transactionBody.getFileID());
        // we have an address book update, refresh the local file
        if (isFileAddressBook(transactionBody.getFileID())) {
            try {
                networkAddressBook.append(contents);
            } catch (IOException e) {
                throw new ParserException("Error appending to network address book", e);
            }
        }
    }

    private void insertCryptoAddClaim(long consensusTimestamp,
                                      CryptoAddClaimTransactionBody transactionBody) {
        if (parserProperties.getPersist().isClaims()) {
            byte[] claim = transactionBody.getClaim().getHash().toByteArray();
            recordParsedItemHandler.onLiveHash(new LiveHash(consensusTimestamp, claim));
        }
    }

    private void insertContractCall(long consensusTimestamp,
                                    ContractCallTransactionBody transactionBody,
                                    TransactionRecord transactionRecord) {
        if (parserProperties.getPersist().isContracts()) {
            byte[] functionParams = transactionBody.getFunctionParameters().toByteArray();
            long gasSupplied = transactionBody.getGas();
            byte[] callResult = new byte[0];
            long gasUsed = 0;
            if (transactionRecord.hasContractCallResult()) {
                callResult = transactionRecord.getContractCallResult().toByteArray();
                gasUsed = transactionRecord.getContractCallResult().getGasUsed();
            }
            insertContractResults(consensusTimestamp, functionParams, gasSupplied, callResult, gasUsed);
        }
    }

    private void insertContractCreateInstance(long consensusTimestamp,
                                              ContractCreateTransactionBody transactionBody,
                                              TransactionRecord transactionRecord) {
        if (parserProperties.getPersist().isContracts()) {
            byte[] functionParams = transactionBody.getConstructorParameters().toByteArray();
            long gasSupplied = transactionBody.getGas();
            byte[] callResult = new byte[0];
            long gasUsed = 0;
            if (transactionRecord.hasContractCreateResult()) {
                callResult = transactionRecord.getContractCreateResult().toByteArray();
                gasUsed = transactionRecord.getContractCreateResult().getGasUsed();
            }
            insertContractResults(consensusTimestamp, functionParams, gasSupplied, callResult, gasUsed);
        }
    }

    private void insertTransferList(long consensusTimestamp, TransferList transferList) {
        for (int i = 0; i < transferList.getAccountAmountsCount(); ++i) {
            var aa = transferList.getAccountAmounts(i);
            var accountId = aa.getAccountID();
            createEntity(getEntity(aa.getAccountID()));
            addCryptoTransferList(consensusTimestamp, accountId.getRealmNum(), accountId.getAccountNum(), aa
                    .getAmount());
        }
    }

    private void insertCryptoCreateTransferList(long consensusTimestamp,
                                                TransactionRecord txRecord,
                                                TransactionBody body,
                                                AccountID createdAccountId,
                                                AccountID payerAccountId) {

        long initialBalance = 0;
        long createdAccountNum = 0;

        // no need to add missing initial balance to transfer list if this is realm and shard <> 0
        boolean addInitialBalance = (txRecord.getReceipt().getAccountID().getShardNum() == 0) && (txRecord.getReceipt()
                .getAccountID().getRealmNum() == 0);

        if (addInitialBalance) {
            initialBalance = body.getCryptoCreateAccount().getInitialBalance();
            createdAccountNum = txRecord.getReceipt().getAccountID().getAccountNum();
        }
        TransferList transferList = txRecord.getTransferList();
        for (int i = 0; i < transferList.getAccountAmountsCount(); ++i) {
            var aa = transferList.getAccountAmounts(i);
            var accountId = aa.getAccountID();
            long accountNum = accountId.getAccountNum();
            createEntity(getEntity(accountId));
            addCryptoTransferList(consensusTimestamp, accountId.getRealmNum(), accountNum, aa.getAmount());

            if (addInitialBalance && (initialBalance == aa.getAmount()) && (accountNum == createdAccountNum)) {
                addInitialBalance = false;
            }
        }

        if (addInitialBalance) {
            createEntity(getEntity(payerAccountId));
            addCryptoTransferList(consensusTimestamp, payerAccountId.getRealmNum(), payerAccountId
                    .getAccountNum(), -initialBalance);

            createEntity(getEntity(createdAccountId));
            addCryptoTransferList(consensusTimestamp, createdAccountId
                    .getRealmNum(), createdAccountNum, initialBalance);
        }
    }

    private void addCryptoTransferList(long consensusTimestamp, long realmNum, long accountNum, long amount) {
        recordParsedItemHandler
                .onCryptoTransferList(new CryptoTransfer(consensusTimestamp, amount, realmNum, accountNum));
    }

    private void insertFileUpdate(long consensusTimestamp, FileUpdateTransactionBody transactionBody) {
        FileID fileId = transactionBody.getFileID();
        byte[] contents = transactionBody.getContents().toByteArray();
        insertFileData(consensusTimestamp, contents, fileId);
        // we have an address book update, refresh the local file
        if (isFileAddressBook(fileId)) {
            try {
                networkAddressBook.update(contents);
            } catch (IOException e) {
                throw new ParserException("Error appending to network address book", e);
            }
        }
    }

    private void insertContractResults(
            long consensusTimestamp, byte[] functionParams, long gasSupplied, byte[] callResult, long gasUsed) {
        recordParsedItemHandler.onContractResult(
                new ContractResult(consensusTimestamp, functionParams, gasSupplied, callResult, gasUsed));
    }

    private Entities getEntity(long shardNum, long realmNum, long entityNum, String type) {
        return entityRepository.findByPrimaryKey(shardNum, realmNum, entityNum).orElseGet(() -> {
            Entities entity = new Entities();
            entity.setEntityNum(entityNum);
            entity.setEntityRealm(realmNum);
            entity.setEntityShard(shardNum);
            entity.setEntityTypeId(entityTypeRepository.findByName(type).map(EntityType::getId).get());
            return entity;
        });
    }

    public EntityId getEntityId(AccountID accountID) {
        return getEntityId(accountID.getShardNum(), accountID.getRealmNum(), accountID.getAccountNum(), "account");
    }

    private EntityId getEntityId(long shardNum, long realmNum, long entityNum, String type) {
        if (0 == entityNum) {
            return null;
        }
        return entityRepository.findEntityIdByNativeIds(shardNum, realmNum, entityNum).orElseGet(() -> {
            Entities entityId = new Entities();
            entityId.setEntityShard(shardNum);
            entityId.setEntityRealm(realmNum);
            entityId.setEntityNum(entityNum);
            entityId.setEntityTypeId(entityTypeRepository.findByName(type).map(EntityType::getId).get());
            return entityRepository.saveAndCacheEntityId(entityId);
        });
    }

    private Entities createEntity(Entities entity) {
        if (entity != null && entity.getId() == null) {
            log.debug("Creating entity: {}", () -> entity.getDisplayId());
            var result = entityRepository.save(entity);
            var entityId = new EntityId(result.getId(), result.getEntityShard(), result.getEntityRealm(),
                    result.getEntityNum(), result.getEntityTypeId());
            entityRepository.cache(entityId);
            return result;
        }
        return entity;
    }

    public enum INIT_RESULT {
        OK, FAIL, SKIP
    }
}
