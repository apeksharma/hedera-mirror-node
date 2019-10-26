package com.hedera.mirror.connector;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import javax.inject.Named;
import javax.validation.constraints.NotBlank;

@Data
@Named
@Validated
@ConfigurationProperties("hedera.mirror.connector.gcp")
public class GCPConnectorProperties {
    private final CommonConnectorProperties commonConnectorProperties;

    @NotBlank
    private String endpoint = "https://www.googleapis.com";

    @NotBlank
    private String bucketName;
}
