package com.hedera.mirror.importer.downloader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Object;

import com.hedera.mirror.importer.config.MirrorImporterConfiguration;

public class RequesterPayBucketTest {

    // AWS Setup
//    private static final String REGION = "us-west-2";
//    private static final String ACCESS_KEY = "AKIA4FLCJJBVILIKZDRE";
//    private static final String SECRET_KEY = "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";
//    private static final String PUBLIC_BUCKET = "appy1";
//    private static final String REQUESTER_PAYS_BUCKET = "appy1-requester-pays";
//    private static final CommonDownloaderProperties.CloudProvider CLOUD_PROVIDER =
//            CommonDownloaderProperties.CloudProvider.S3;

    // GCP Setup
    private static final String REGION = "region";
    private static final String ACCESS_KEY = "GOOGMZBTWGIRJPYQ7FUS4WIS";
    private static final String SECRET_KEY = "M+gazKbO7SKp78wDOhdiLQ1sG+rJ3rvH4D5hvob/";
    private static final String PUBLIC_BUCKET = "hedera-test-bucket";
    private static final String REQUESTER_PAYS_BUCKET = "hedera-test-bucket-requester-pays";
    private static final CommonDownloaderProperties.CloudProvider CLOUD_PROVIDER =
            CommonDownloaderProperties.CloudProvider.GCP;

    private S3AsyncClient anonymousClient;
    private S3AsyncClient authClient;

    @BeforeEach
    void beforeEach() {
        CommonDownloaderProperties downloaderProperties = new CommonDownloaderProperties();
        downloaderProperties.setRegion(REGION);
        downloaderProperties.setCloudProvider(CLOUD_PROVIDER);

        // Setup anonymous client
        anonymousClient = new MirrorImporterConfiguration(downloaderProperties).s3AsyncClient();

        // Setup authenticated client
        downloaderProperties.setAccessKey(ACCESS_KEY);
        downloaderProperties.setSecretKey(SECRET_KEY);
        authClient = new MirrorImporterConfiguration(downloaderProperties).s3AsyncClient();
    }

    // Current state
    @Test
    void simpleRequestOnPublicBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(PUBLIC_BUCKET)
                .build();
        assertListResult(anonymousClient.listObjects(listRequest));
    }

    // During transitioning
    @Test
    void requesterPaysRequestOnPublicBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(PUBLIC_BUCKET)
                .requestPayer(RequestPayer.REQUESTER)
                .build();
        assertListResult(authClient.listObjects(listRequest));
    }

    // Desired final state
    @Test
    void requesterPaysRequestOnRequesterPaysBucketWorks() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(REQUESTER_PAYS_BUCKET)
                .requestPayer(RequestPayer.REQUESTER)
                .build();
        assertListResult(authClient.listObjects(listRequest));
    }

    @Test
    void simpleRequestOnRequesterPaysBucketFails() throws Exception {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(REQUESTER_PAYS_BUCKET)
                .build();
        CompletableFuture<ListObjectsResponse> response = anonymousClient.listObjects(listRequest);
        Assertions.assertThrows(ExecutionException.class, () -> {
            response.get();
        });
    }

    void assertListResult(CompletableFuture<ListObjectsResponse> response) throws Exception {
        List<S3Object> result = response.get().contents();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("1.jpg", result.get(0).key());
    }
}
