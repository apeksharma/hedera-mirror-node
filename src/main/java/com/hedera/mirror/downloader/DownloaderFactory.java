package com.hedera.mirror.downloader;

import com.hedera.mirror.addressbook.NetworkAddressBook;
import com.hedera.mirror.connector.CommonConnectorProperties;
import com.hedera.mirror.connector.GCPConnector;
import com.hedera.mirror.connector.S3Connector;

import com.hedera.mirror.repository.ApplicationStatusRepository;

import lombok.RequiredArgsConstructor;
import javax.inject.Named;

/**
 * Factory to create Downloader for different streams.
 * This does not implement FactoryBean<T> on purpose. Doing so would leave 'streamProperties' uninitialized on creation,
 * and will then need to be set through setter/init() method.
 */
@Named
@RequiredArgsConstructor
public class DownloaderFactory  {
    private final CommonConnectorProperties commonConnectorProperties;
    private final S3Connector s3Connector;
    private final GCPConnector gcpConnector;
    protected final ApplicationStatusRepository applicationStatusRepository;
    private final NetworkAddressBook networkAddressBook;

    public Downloader newDownloaderForStream(StreamProperties streamProperties) {
        var provider = commonConnectorProperties.getProvider();
        if (provider == CommonConnectorProperties.Provider.S3) {
            return new Downloader(s3Connector, applicationStatusRepository, networkAddressBook, streamProperties);
        } else if (provider == CommonConnectorProperties.Provider.GCP) {
            return new Downloader(gcpConnector, applicationStatusRepository, networkAddressBook, streamProperties);
        } else {
            throw new IllegalArgumentException("Unexpected provider : " + provider);
        }
    }
}
