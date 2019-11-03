package com.hedera.mirror.parser.balance;

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

import com.hedera.IntegrationTest;

import com.hedera.mirror.domain.StreamItem;

import com.hedera.mirror.domain.StreamType;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;
import javax.transaction.Transactional;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@Transactional
public class AccountBalancesFileLoaderTest extends IntegrationTest {

    @Value("classpath:data/accountBalances/balance0.0.3/2019-08-30T18_15_00.016002001Z_Balances.csv")
    private Path path;

    @Resource
    private BalanceParserProperties parserProperties;

    @Test
    public void positiveSmallFile() throws Exception {
        StreamItem streamItem = new StreamItem(StreamItem.Type.PAYLOAD, "2019-08-30T18_15_00.016002001Z_Balances.csv",
                "0.0.3", StreamType.BALANCE);
        streamItem.setDataBytes(ByteBuffer.wrap(Files.readAllBytes(path)));

        // The test has a 2 line header and 2 data lines.
        final var fileLoader = new AccountBalancesFileLoader(parserProperties, streamItem);
        boolean success = fileLoader.loadAccountBalances();
        assertAll(
                () -> assertEquals(25391, fileLoader.getValidRowCount())
                ,() -> assertTrue(success)
        );
        // TODO assert the rows actually added to the database.
    }
}
