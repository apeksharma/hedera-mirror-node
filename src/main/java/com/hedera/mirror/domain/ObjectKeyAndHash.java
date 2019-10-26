package com.hedera.mirror.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ObjectKeyAndHash {
    String objectKey;
    String md5; // hex encoded md5 checksum
}
