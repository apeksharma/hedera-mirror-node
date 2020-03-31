package com.hedera.mirror.importer.parser.domain;

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

import com.google.protobuf.InvalidProtocolBufferException;

import com.hedera.mirror.importer.exception.ParserException;

import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
public class RecordItem implements StreamItem {
    private final Transaction transaction;
    private final TransactionBody transactionBody;
    private final TransactionRecord record;
    private final byte[] transactionBytes;
    private final byte[] recordBytes;

    public RecordItem(Transaction transaction, TransactionRecord record) {
        this(transaction, record, null, null);
    }

    public RecordItem(Transaction transaction, TransactionRecord record, byte[] transactionBytes, byte[] recordBytes) {
        this.transaction = transaction;
        this.transactionBody = parseTransactionBody(transaction);
        this.record = record;
        this.transactionBytes = transactionBytes;
        this.recordBytes = recordBytes;
    }

    // TODO: add test
    private static TransactionBody parseTransactionBody(Transaction transaction){
        if (transaction.hasBody()) {
            return transaction.getBody();
        } else {
            try {
                return TransactionBody.parseFrom(transaction.getBodyBytes());
            } catch (InvalidProtocolBufferException e) {
                throw new ParserException("Error parsing transaction from body bytes", e);
            }
        }
    }
}
