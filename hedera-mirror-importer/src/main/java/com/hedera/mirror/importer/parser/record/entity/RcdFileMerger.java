package com.hedera.mirror.importer.parser.record.entity;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import lombok.extern.log4j.Log4j2;

import com.hedera.mirror.importer.domain.RecordFile;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.parser.domain.RecordItem;
import com.hedera.mirror.importer.parser.domain.StreamFileData;
import com.hedera.mirror.importer.parser.record.RecordItemListener;
import com.hedera.mirror.importer.parser.record.RecordStreamFileListener;
import com.hedera.mirror.importer.util.FileDelimiter;
import com.hedera.mirror.importer.util.Utility;

@Log4j2
//@Named
@ConditionOnEntityRecordParser
public class RcdFileMerger implements RecordItemListener, RecordStreamFileListener {
    static final int NUM_FILES_MERGE = 20;

    DataOutputStream fileWriter;
    int filesMerged = 0;

    @Override
    public void onStart(StreamFileData streamFileData) throws ImporterException {
        if (filesMerged == 0) {
            String filename = "/tmp/" + Utility.getFileName(streamFileData.getFilename());
            log.info("New output file {}", filename);
            try {
                fileWriter = new DataOutputStream(new FileOutputStream(filename));
                fileWriter.writeInt(2); // recordFormatVersion
                fileWriter.writeInt(2); // version
                fileWriter.writeByte(FileDelimiter.RECORD_TYPE_PREV_HASH);
                fileWriter.write(new byte[48]); // prev hash. check will be skipped
            } catch (Exception e) {
                log.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onEnd(RecordFile fileInfo) throws ImporterException {
        try {
            fileWriter.flush();
            filesMerged = (filesMerged + 1) % NUM_FILES_MERGE;
            log.info("Parsed file {} (merge count = {})", fileInfo.getName(), filesMerged);
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onError() {
    }

    @Override
    public void onItem(RecordItem recordItem) throws ImporterException {
        try {
            fileWriter.writeByte(FileDelimiter.RECORD_TYPE_RECORD);
            fileWriter.writeInt(recordItem.getTransactionBytes().length);
            fileWriter.write(recordItem.getTransactionBytes());
            fileWriter.writeInt(recordItem.getRecordBytes().length);
            fileWriter.write(recordItem.getRecordBytes());
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
}
