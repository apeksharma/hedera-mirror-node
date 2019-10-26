package com.hedera.mirror.connector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GCPConnectorTest {

    // TODO: Use GCP mock, if exists, to test gcp connector.

    @Test
    void testPageToken() throws Exception {
        GCPConnector gcpConnector = new GCPConnector(new GCPConnectorProperties(new CommonConnectorProperties()));
        assertThat(gcpConnector.getPageToken("recordstreams/record0.0.3/2019-10-27T00_41_47.735805Z.rcd_sig"))
                .isEqualTo("Cj1yZWNvcmRzdHJlYW1zL3JlY29yZDAuMC4zLzIwMTktMTAtMjdUMDBfNDFfNDcuNzM1ODA1Wi5yY2Rfc2ln");

        assertThat(gcpConnector.getPageToken("recordstreams/record0.0.3/2019-10-27T02_46_45.902109Z.rcd"))
                .isEqualTo("CjlyZWNvcmRzdHJlYW1zL3JlY29yZDAuMC4zLzIwMTktMTAtMjdUMDJfNDZfNDUuOTAyMTA5Wi5yY2Q=");
    }
}
