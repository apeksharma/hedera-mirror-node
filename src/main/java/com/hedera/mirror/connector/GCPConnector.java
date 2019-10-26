package com.hedera.mirror.connector;

import com.google.common.primitives.Bytes;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.google.gson.JsonParser;

import com.hedera.mirror.domain.ObjectKeyAndHash;
import com.hedera.mirror.domain.StreamType;

import com.hedera.mirror.downloader.PendingDownload;

import com.hedera.utilities.Utility;

import lombok.extern.log4j.Log4j2;

import javax.inject.Named;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

@Log4j2
@Named
public class GCPConnector implements Connector {
    private final GCPConnectorProperties gcpConnectorProperties;
    private final HttpClient httpClient;
    // Example query: GET https://www.googleapis.com/storage/v1/b/hedera-mainnet-streams/o?maxResults=999&pageToken
    // =Cj1yZWNvcmRzdHJlYW1zL3JlY29yZDAuMC4zLzIwMTktMTAtMjdUMDJfMDVfMDUuMDE5NTU2Wi5yY2Rfc2ln&prefix=recordstreams
    // %2Frecord0.0.3%2F&fields=items(name%2Cmd5Hash) HTTP/1.1
    private final static String listRequestTemplate = "%s/storage/v1/b/%s/o?maxResults=%d&pageToken=%s&prefix=%s&fields=%s";
    private final static String getObjectTemplate =  "%s/storage/v1/b/%s/o/%s?alt=media";
    private final static Charset utf8 = StandardCharsets.UTF_8;

    public GCPConnector(GCPConnectorProperties gcpConnectorProperties) {
        this.gcpConnectorProperties = gcpConnectorProperties;
        httpClient = HttpClient.newHttpClient();
    }

    @Override
    public List<ObjectKeyAndHash> List(StreamType streamType, String nodeAccountId, String fromFileName, int maxCount) {
        String prefix = getPrefix(streamType) + nodeAccountId + "/";
        String uri = String.format(
                listRequestTemplate, gcpConnectorProperties.getEndpoint(), gcpConnectorProperties.getBucketName(),
                maxCount, getPageToken(prefix + fromFileName), URLEncoder.encode(prefix, utf8),
                URLEncoder.encode("items(name,md5Hash)", utf8));
        var request = HttpRequest.newBuilder().uri(URI.create(uri)).build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            log.error("Exception occurred when listing stream {}, node {}\nException:{}", streamType, nodeAccountId, e);
            return new ArrayList<>();
        }
        JsonObject obj = new JsonParser().parse(response.body()).getAsJsonObject();
        Iterator<JsonElement> itr = obj.getAsJsonArray("items").iterator();
        List<ObjectKeyAndHash> result = new ArrayList<>(maxCount);
        while (itr.hasNext()) {
            JsonObject item = itr.next().getAsJsonObject();
            result.add(new ObjectKeyAndHash(
                    item.getAsJsonPrimitive("name").getAsString(), item.getAsJsonPrimitive("md5Hash").getAsString()));
        }
        log.debug("List on stream {}, node {}, from {}, maxcount{} - found {} items",
                streamType, nodeAccountId, fromFileName, maxCount, result.size());
        return result;
    }

    @Override
    public PendingDownload GetObjectByKey(String objectKey, Path localFile) {
        Utility.ensureDirectory(localFile.getParent());
        String uri = String.format(getObjectTemplate, gcpConnectorProperties.getEndpoint(),
                gcpConnectorProperties.getBucketName(), URLEncoder.encode(objectKey, utf8));
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(uri)).build();
        var completableFuture = httpClient.sendAsync(
                request, HttpResponse.BodyHandlers.ofFile(localFile)).thenApply(HttpResponse::body);
        return new PendingDownload(completableFuture, localFile.toFile(), objectKey);
    }

    @Override
    public PendingDownload Get(StreamType streamType, String nodeAccountId, String fileName, Path localFile) {
        String s3ObjectKey = getPrefix(streamType) + nodeAccountId + "/" + fileName;
        return GetObjectByKey(s3ObjectKey, localFile);
    }

    private String getPrefix(StreamType streamType) {
        switch(streamType) {
            case BALANCE:
                return gcpConnectorProperties.getCommonConnectorProperties().getBalancePrefix();
            case EVENT:
                return gcpConnectorProperties.getCommonConnectorProperties().getEventPrefix();
            case RECORD:
                return gcpConnectorProperties.getCommonConnectorProperties().getRecordPrefix();
            default:
                throw new IllegalArgumentException("Wrong StreamType: " + streamType);
        }
    }

    // TODO: comment explaining logic
    String getPageToken(String filename) {
        if (filename.length() > 127) {
            throw new IllegalArgumentException("TODO: figure encoding of large filenames to pageToken");
        }
        byte len = (byte)filename.length();
        byte[] prefix = {0x0a, len};
        byte[] data = Bytes.concat(prefix, filename.getBytes(StandardCharsets.US_ASCII));

        // base64 encoding with padding - (0x0a + length in hex + data)
        return Base64.getEncoder().encodeToString(data);
    }
}
