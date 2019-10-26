package com.hedera.mirror.connector;

import com.hedera.mirror.domain.ObjectKeyAndHash;
import com.hedera.mirror.domain.StreamType;
import com.hedera.mirror.downloader.PendingDownload;

import java.nio.file.Path;
import java.util.List;

public interface Connector {
    /**
     * List new files in the cloud storage.
     * @param fromFileName start listing from the given file name
     * @return List of FileNameAndHash fetched from the cloud storage.
     */
    List<ObjectKeyAndHash> List(StreamType streamType, String nodeAccountId, String fromFileName, int maxCount);

    /**
     * Gets contents of the given objectKey from the cloud storage
     * @param localFile Path to local file where the downloaded contents will be saved.
     * @return a PendingDownload for which the caller can waitForCompletion() to wait for the download to complete.
     */
    PendingDownload GetObjectByKey(String objectKey, Path localFile);

    /**
     * Uses streamType, nodeAccountId, and filename to generate object key, then gets contents from the cloud storage.
     * @param localFile Path to local file where the downloaded contents will be saved.
     * @return a PendingDownload for which the caller can waitForCompletion() to wait for the download to complete.
     */
    PendingDownload Get(StreamType streamType, String nodeAccountId, String fileName, Path localFile);
}
