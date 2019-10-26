package com.hedera.mirror.connector;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import com.hedera.mirror.domain.ObjectKeyAndHash;

import com.hedera.mirror.domain.StreamType;
import com.hedera.mirror.downloader.PendingDownload;
import com.hedera.utilities.Utility;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Log4j2
@Named
public class S3Connector implements Connector {

    private final S3ConnectorProperties s3ConnectorProperties;

    private final TransferManager transferManager;

    private final static Comparator<String> s3KeyComparator = (String o1, String o2) -> {
        Instant o1TimeStamp = Utility.parseToInstant(Utility.parseS3SummaryKey(o1).getMiddle());
        Instant o2TimeStamp = Utility.parseToInstant(Utility.parseS3SummaryKey(o2).getMiddle());
        if (o1TimeStamp == null) return -1;
        if (o2TimeStamp == null) return 1;
        return o1TimeStamp.compareTo(o2TimeStamp);
    };

    public S3Connector(S3ConnectorProperties s3ConnectorProperties) {
        this.s3ConnectorProperties = s3ConnectorProperties;

        // Build S3 client
        var s3ClientBuilder =  AmazonS3ClientBuilder.standard();

        RetryPolicy retryPolicy = PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(5);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setMaxConnections(s3ConnectorProperties.getMaxConnections());
        s3ClientBuilder.withClientConfiguration(clientConfiguration);

        s3ClientBuilder.withRequestHandlers(new RequestHandler2() {
            private Logger logger = LogManager.getLogger(AmazonS3Client.class);

            @Override
            public void afterError(Request<?> request, Response<?> response, Exception e) {
                logger.error("Error calling {} {}", request.getHttpMethod(), request.getEndpoint(), e);
            }
        });

        AWSCredentials awsCredentials = new AnonymousAWSCredentials();
        if (StringUtils.isNotBlank(s3ConnectorProperties.getAccessKey()) &&
                StringUtils.isNotBlank(s3ConnectorProperties.getSecretKey())) {
            awsCredentials = new BasicAWSCredentials(
                    s3ConnectorProperties.getAccessKey(), s3ConnectorProperties.getSecretKey());
        }
        s3ClientBuilder.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));

        s3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                s3ConnectorProperties.getEndpoint(), s3ConnectorProperties.getRegion()));

        this.transferManager = TransferManagerBuilder.standard()
                .withExecutorFactory(() -> new ThreadPoolExecutor(
                        s3ConnectorProperties.getMinThreads(), s3ConnectorProperties.getMaxThreads(), 120,
                        TimeUnit.SECONDS, new ArrayBlockingQueue<>(s3ConnectorProperties.getMaxQueued())))
                .withS3Client(s3ClientBuilder.build())
                .build();
        Runtime.getRuntime().addShutdownHook(new Thread(transferManager::shutdownNow));
    }

    @Override
    public List<ObjectKeyAndHash> List(StreamType streamType, String nodeAccountId, String fromFileName, int maxCount) {
        String prefix = getPrefix(streamType) + nodeAccountId + "/";
        ListObjectsRequest listRequest = new ListObjectsRequest()
                .withBucketName(s3ConnectorProperties.getBucketName())
                .withPrefix(prefix)
                .withDelimiter("/")
                .withMarker(prefix + fromFileName)
                .withMaxKeys(maxCount);
        ObjectListing objects = transferManager.getAmazonS3Client().listObjects(listRequest);
        List<S3ObjectSummary> summaries = objects.getObjectSummaries();
        List<ObjectKeyAndHash> result = new ArrayList<>(maxCount);
        for (S3ObjectSummary summary : summaries) {
            if (s3KeyComparator.compare(summary.getKey(), prefix + fromFileName) > 0 || fromFileName.isEmpty()) {
                result.add(new ObjectKeyAndHash(summary.getKey(), summary.getETag()));
            }
        }
        return result;
    }


    @Override
    public PendingDownload GetObjectByKey(String objectKey, Path localFile) {
        File file = localFile.toFile();
        Download download = transferManager.download(s3ConnectorProperties.getBucketName(), objectKey, file);
        return new PendingDownload(download, file, objectKey);
    }

    @Override
    public PendingDownload Get(StreamType streamType, String nodeAccountId, String fileName, Path localFile) {
        String s3ObjectKey = getPrefix(streamType) + nodeAccountId + "/" + fileName;
        return GetObjectByKey(s3ObjectKey, localFile);
    }

    private String getPrefix(StreamType streamType) {
        switch(streamType) {
            case BALANCE:
                return s3ConnectorProperties.getCommonConnectorProperties().getBalancePrefix();
            case EVENT:
                return s3ConnectorProperties.getCommonConnectorProperties().getEventPrefix();
            case RECORD:
                return s3ConnectorProperties.getCommonConnectorProperties().getRecordPrefix();
            default:
                throw new IllegalArgumentException("Wrong StreamType: " + streamType);
        }
    }
}
