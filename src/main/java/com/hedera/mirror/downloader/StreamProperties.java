package com.hedera.mirror.downloader;

import com.hedera.mirror.domain.ApplicationStatusCode;
import com.hedera.mirror.domain.StreamType;
import com.hedera.utilities.Utility;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import javax.annotation.PostConstruct;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;

@Validated
@Data
@RequiredArgsConstructor
public abstract class StreamProperties {
    /* ********** YML configurable parameters ************* */
    /** If true, downloader will fetch and validate data for this stream, and then push to parser */
    private boolean enabled = true;

    /** Number of sig files (per node account id) to download in parallel. */
    @Min(1)
    private int batchSize;

    /** Time to wait between consecutive calls to {@code Downloader#download()}. */
    @NotNull
    private Duration frequency;
    /* ****** End of configurable parameters ************** */

    @NotNull
    private final StreamType streamType;

    @NotNull
    private final Path dataPath;

    @NotNull
    private final ApplicationStatusCode lastValidDownloadedFileKey;

    private final ApplicationStatusCode lastValidDownloadedFileHashKey;  // Used only if verifyHashChain is true

    private final ApplicationStatusCode bypassHashKey;  // Used only if verifyHashChain is true

    private final boolean verifyHashChain;

    // Used only if verifyHashChain is true
    protected abstract String getPrevFileHash(String filePath);

    public Path getStreamPath() {
        return dataPath.resolve(getStreamType().getPath());
    }

    public Path getTempPath() {
        return getStreamPath().resolve(getStreamType().getTemp());
    }

    public Path getValidPath() {
        return getStreamPath().resolve(getStreamType().getValid());
    }

    @PostConstruct
    public void init() {
        Utility.ensureDirectory(getTempPath());
        Utility.ensureDirectory(getValidPath());
        Utility.purgeDirectory(getTempPath());
    }
}
