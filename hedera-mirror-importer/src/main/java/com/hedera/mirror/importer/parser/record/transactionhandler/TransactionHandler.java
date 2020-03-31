package com.hedera.mirror.importer.parser.record.transactionhandler;

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

import com.hedera.mirror.importer.domain.Entities;
import com.hedera.mirror.importer.domain.EntityId;
import com.hedera.mirror.importer.domain.Transaction;
import com.hedera.mirror.importer.parser.domain.RecordItem;


public interface TransactionHandler {
    /**
     * Parse the main entity associated with this transaction.
     */
    EntityId getId(RecordItem recordItem);

    default EntityId getProxyEntityId(RecordItem recordItem) {
        return null;
    }

    default Transaction createTransaction(RecordItem recordItem) {
        return new Transaction();
    }

    /**
     * @return true if transaction may be updating the entity; else false. If this function returns true, then {@link
     * #updateEntity(Entities, RecordItem)} will be called.
     */
    default boolean updatesEntity() {
        return false;
    }

    default void updateEntity(Entities entity, RecordItem recordItem) {
    }

    // TopicMessage, CTL, Contract, File, etc
    default void updateTransaction(Transaction transaction, RecordItem recordItem) {}
}
