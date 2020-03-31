package com.hedera.mirror.importer.parser.record.transactionhandler;

import com.hederahashgraph.api.proto.java.TransactionBody;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;

@Named
@RequiredArgsConstructor
public class TransactionHandlerFactory {
    private final ConsensusCreateTopicTransactionHandler consensusCreateTopicTransactionHandler;
    private final ConsensusDeleteTopicTransactionHandler consensusDeleteTopicTransactionHandler;
    private final ConsensusSubmitMessageTransactionHandler consensusSubmitMessageTransactionHandler;
    private final ConsensusUpdateTopicTransactionHandler consensusUpdateTopicTransactionHandler;
    private final ContractCallTransactionHandler contractCallTransactionHandler;
    private final ContractCreateTransactionHandler contractCreateTransactionHandler;
    private final ContractDeleteTransactionHandler contractDeleteTransactionHandler;
    private final ContractUpdateTransactionHandler contractUpdateTransactionHandler;
    private final CryptoAddClaimTransactionHandler cryptoAddClaimTransactionHandler;
    private final CryptoCreateTransactionHandler cryptoCreateTransactionHandler;
    private final CryptoDeleteClaimTransactionHandler cryptoDeleteClaimTransactionHandler;
    private final CryptoDeleteTransactionHandler cryptoDeleteTransactionHandler;
    private final CryptoTransferTransactionHandler cryptoTransferTransactionHandler;
    private final CryptoUpdateTransactionHandler cryptoUpdateTransactionHandler;
    private final FileAppendTransactionHandler fileAppendTransactionHandler;
    private final FileCreateTransactionHandler fileCreateTransactionHandler;
    private final FileDeleteTransactionHandler fileDeleteTransactionHandler;
    private final FileUpdateTransactionHandler fileUpdateTransactionHandler;
    private final FreezeTransactionHandler freezeTransactionHandler;
    private final SystemDeleteTransactionHandler systemDeleteTransactionHandler;
    private final SystemUndeleteTransactionHandler systemUndeleteTransactionHandler;
    private final UnknownDataTransactionHandler unknownDataTransactionHandler;

    public TransactionHandler create(TransactionBody body) {
        if (body.hasContractCall()) {
            return contractCallTransactionHandler;
        } else if (body.hasContractCreateInstance()) {
            return contractCreateTransactionHandler;
        } else if (body.hasContractDeleteInstance()) {
            return contractDeleteTransactionHandler;
        } else if (body.hasContractUpdateInstance()) {
            return contractUpdateTransactionHandler;
        } else if (body.hasCryptoCreateAccount()) {
            return cryptoCreateTransactionHandler;
        } else if (body.hasCryptoAddClaim()) {
            return cryptoAddClaimTransactionHandler;
        } else if (body.hasCryptoDelete()) {
            return cryptoDeleteTransactionHandler;
        } else if (body.hasCryptoDeleteClaim()) {
            return cryptoDeleteClaimTransactionHandler;
        } else if (body.hasCryptoUpdateAccount()) {
            return cryptoUpdateTransactionHandler;
        } else if (body.hasFileCreate()) {
            return fileCreateTransactionHandler;
        } else if (body.hasFileAppend()) {
            return fileAppendTransactionHandler;
        } else if (body.hasFileDelete()) {
            return fileDeleteTransactionHandler;
        } else if (body.hasFileUpdate()) {
            return fileUpdateTransactionHandler;
        } else if (body.hasSystemDelete()) {
            return systemDeleteTransactionHandler;
        } else if (body.hasSystemUndelete()) {
            return systemUndeleteTransactionHandler;
        } else if (body.hasConsensusCreateTopic()) {
            return consensusCreateTopicTransactionHandler;
        } else if (body.hasConsensusUpdateTopic()) {
            return consensusUpdateTopicTransactionHandler;
        } else if (body.hasConsensusDeleteTopic()) {
            return consensusDeleteTopicTransactionHandler;
        } else if (body.hasConsensusSubmitMessage()) {
            return consensusSubmitMessageTransactionHandler;
        } else {
            return unknownDataTransactionHandler;
        }
    }
}
