package com.hedera.mirror.connector;

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

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.inject.Named;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@Named
@Validated
@ConfigurationProperties("hedera.mirror.connector")
public class CommonConnectorProperties {
    @NotNull
    private Provider provider = Provider.S3;

    @NotBlank
    private String balancePrefix = "accountBalances/balance";

    @NotBlank
    private String eventPrefix = "eventsStreams/events_";

    @NotBlank
    private String recordPrefix = "recordstreams/record";

    @Getter
    @RequiredArgsConstructor
    public enum Provider {
        S3,
        GCP,
    }
}
