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

import com.hedera.mirror.domain.StreamItem;

import lombok.extern.log4j.Log4j2;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import javax.inject.Named;

@Log4j2
@Named
public class BalanceFileParser implements MessageHandler {
    private final BalanceParserProperties parserProperties;

	public BalanceFileParser(BalanceParserProperties parserProperties) {
	    this.parserProperties = parserProperties;
	}

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        StreamItem streamItem = (StreamItem) message.getPayload();
        log.debug("Processing {}", streamItem);
        try {
			if (new AccountBalancesFileLoader(parserProperties, streamItem).loadAccountBalances()) {
			    // TODO: write state to applicationStatusRepository.
			}
		} catch (Exception e) {
			log.error("Error processing balances stream item {}", streamItem, e);
		}
	}
}
