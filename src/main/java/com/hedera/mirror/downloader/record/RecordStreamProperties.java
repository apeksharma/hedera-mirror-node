package com.hedera.mirror.downloader.record;

import com.hedera.mirror.MirrorProperties;
import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.StreamType;

import com.hedera.mirror.downloader.StreamProperties;

import com.hedera.mirror.parser.record.RecordFileParser;

import org.springframework.boot.context.properties.ConfigurationProperties;
import javax.inject.Named;
import java.time.Duration;

@Named
@ConfigurationProperties("hedera.mirror.downloader.record")
public class RecordStreamProperties extends StreamProperties {
    public RecordStreamProperties(MirrorProperties mirrorProperties) {
        super(StreamType.RECORD, mirrorProperties.getDataPath(),
                ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE,
                ApplicationStatusCode.LAST_VALID_DOWNLOADED_RECORD_FILE_HASH,
                ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER,
                true);
        // set defaults
        this.setBatchSize(40);
        this.setFrequency(Duration.ofMillis(500L));
    }

    @Override
    protected String getPrevFileHash(String filePath) {
        return RecordFileParser.readPrevFileHash(filePath);
    }
}
