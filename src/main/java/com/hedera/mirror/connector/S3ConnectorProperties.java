package com.hedera.mirror.connector;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import javax.inject.Named;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@Data
@Named
@Validated
@ConfigurationProperties("hedera.mirror.connector.s3")
public class S3ConnectorProperties {
    private final CommonConnectorProperties commonConnectorProperties;

    @NotBlank
    private String endpoint = "https://s3.amazonaws.com";

    @NotBlank
    private String bucketName;

    private String region = "us-east-1";

    private String accessKey;
    private String secretKey;

    @Min(0)
    private int maxConnections = 500;

    @Min(1)
    private int maxQueued = 500;

    @Min(1)
    private int maxThreads = 60;

    @Min(0)
    private int minThreads = 20;
}
