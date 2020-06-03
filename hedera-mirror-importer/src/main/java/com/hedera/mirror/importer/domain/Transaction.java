package com.hedera.mirror.importer.domain;

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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import com.hedera.mirror.importer.converter.ByteArraySerializer;
import com.hedera.mirror.importer.converter.EntityIdConverter;
import com.hedera.mirror.importer.converter.EntityIdSerializer;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@ToString(exclude = {"memo", "transactionHash", "transactionBytes"})
public class Transaction {

    @Id
    private Long consensusNs;

    @Convert(converter = EntityIdConverter.class)
    @JsonSerialize(using = EntityIdSerializer.class)
    private EntityId nodeAccountId;

    @JsonSerialize(using = ByteArraySerializer.class)
    private byte[] memo;

    private Integer type;

    private Integer result;

    @Convert(converter = EntityIdConverter.class)
    @JsonSerialize(using = EntityIdSerializer.class)
    private EntityId payerAccountId;

    private Long chargedTxFee;

    private Long initialBalance;

    @Convert(converter = EntityIdConverter.class)
    @JsonSerialize(using = EntityIdSerializer.class)
    private EntityId entityId;

    private Long validStartNs;

    private Long validDurationSeconds;

    private Long maxFee;

    @JsonSerialize(using = ByteArraySerializer.class)
    private byte[] transactionHash;

    @JsonSerialize(using = ByteArraySerializer.class)
    private byte[] transactionBytes;
}
