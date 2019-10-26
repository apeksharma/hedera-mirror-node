package com.hedera.mirror.downloader.event;

import com.hedera.mirror.MirrorProperties;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.StreamType;
import com.hedera.mirror.downloader.StreamProperties;

import com.hedera.mirror.parser.event.EventStreamFileParser;

import org.springframework.boot.context.properties.ConfigurationProperties;
import javax.inject.Named;
import java.time.Duration;

@Named
@ConfigurationProperties("hedera.mirror.downloader.event")
public class EventStreamProperties extends StreamProperties {
    public EventStreamProperties(MirrorProperties mirrorProperties) {
        super(StreamType.EVENT, mirrorProperties.getDataPath(),
                ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE,
                ApplicationStatusCode.LAST_VALID_DOWNLOADED_EVENT_FILE_HASH,
                ApplicationStatusCode.EVENT_HASH_MISMATCH_BYPASS_UNTIL_AFTER,
                true);
        // set defaults
        this.setBatchSize(15);
        this.setFrequency(Duration.ofMinutes(1L));
    }

    @Override
    protected String getPrevFileHash(String filePath) {
        return EventStreamFileParser.readPrevFileHash(filePath);
    }
}
