package com.hedera.mirror.downloader.balance;

import com.hedera.mirror.MirrorProperties;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.StreamType;
import com.hedera.mirror.downloader.StreamProperties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.inject.Named;
import java.time.Duration;

@Named
@ConfigurationProperties("hedera.mirror.downloader.balance")
public class BalanceStreamProperties extends StreamProperties {
    public BalanceStreamProperties(MirrorProperties mirrorProperties) {
        super(StreamType.BALANCE, mirrorProperties.getDataPath(),
                ApplicationStatusCode.LAST_VALID_DOWNLOADED_BALANCE_FILE, null, null, false);
        // set defaults
        this.setBatchSize(15);
        this.setFrequency(Duration.ofMillis(500L));
    }

    @Override
    protected String getPrevFileHash(String filePath) {
        return null; // not used since verifyHashChain = false
    }
}
