package com.hedera.mirror.downloader;

import com.hedera.FileCopier;
import com.hedera.mirror.MirrorProperties;

import com.hedera.mirror.addressbook.NetworkAddressBook;
import com.hedera.mirror.connector.CommonConnectorProperties;
import com.hedera.mirror.connector.S3Connector;
import com.hedera.mirror.connector.S3ConnectorProperties;
import com.hedera.mirror.domain.HederaNetwork;

import com.hedera.mirror.repository.ApplicationStatusRepository;

import com.hedera.utilities.Utility;

import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public abstract class DownloaderTestingBase {
    @Mock
    protected ApplicationStatusRepository applicationStatusRepository;

    @TempDir
    protected Path dataPath;

    @TempDir
    protected Path s3Path;

    protected S3Mock s3;
    protected FileCopier fileCopier;
    protected MirrorProperties mirrorProperties;
    protected CommonConnectorProperties commonConnectorProperties;
    protected S3ConnectorProperties s3ConnectorProperties;
    protected S3Connector s3Connector;
    protected DownloaderFactory downloaderFactory;
    protected Path validPath;
    protected StreamProperties streamProperties;

    protected abstract StreamProperties getStreamProperties();
    protected abstract void doDownload();
    protected abstract boolean isSigFile(String file);
    protected abstract boolean isDataFile(String file);

    protected void beforeEach(TestInfo testInfo, String testDataDir) {
        System.out.println("Before test: " + testInfo.getTestMethod().get().getName());
        initMirrorProperties();
        initS3();
        downloaderFactory = new DownloaderFactory(commonConnectorProperties, s3Connector, null,
                applicationStatusRepository, new NetworkAddressBook(mirrorProperties));
        streamProperties = getStreamProperties();

        fileCopier = FileCopier.create(Utility.getResource("data").toPath(), s3Path)
                .from(testDataDir)
                .to(s3ConnectorProperties.getBucketName(), streamProperties.getStreamType().getPath());

        validPath = streamProperties.getValidPath();
    }

    protected void afterEach(TestInfo testInfo) {
        s3.shutdown();
        System.out.println("After test: " + testInfo.getTestMethod().get().getName());
        System.out.println("##########################################\n");
    }

    private void initMirrorProperties() {
        mirrorProperties = new MirrorProperties();
        mirrorProperties.setDataPath(dataPath);
        mirrorProperties.setNetwork(HederaNetwork.TESTNET);
    }

    private void initS3() {
        commonConnectorProperties = new CommonConnectorProperties();
        s3ConnectorProperties = new S3ConnectorProperties(commonConnectorProperties);
        s3ConnectorProperties.setEndpoint("http://127.0.0.1:8001");
        s3ConnectorProperties.setBucketName("test");

        s3Connector = new S3Connector(s3ConnectorProperties);

        s3 = S3Mock.create(8001, s3Path.toString());
        s3.start();
    }

    @Test
    @DisplayName("Missing address book")
    void testMissingAddressBook() throws Exception {
        Files.delete(mirrorProperties.getAddressBookPath());
        fileCopier.copy();
        doReturn("").when(applicationStatusRepository).findByStatusCode(streamProperties.getLastValidDownloadedFileKey());
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    protected void testMaxDownloadItemsReached(String filename) throws Exception {
        streamProperties.setBatchSize(1);
        fileCopier.copy();
        when(applicationStatusRepository.findByStatusCode(streamProperties.getLastValidDownloadedFileKey())).thenReturn("");
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(1)
                .allMatch(p -> isDataFile(p.toString()))
                .extracting(Path::getFileName)
                .contains(Paths.get(filename));
    }

    @Test
    @DisplayName("Missing signatures")
    void missingSignatures() throws Exception {
        fileCopier.filterFiles(file -> isDataFile(file.getName())).copy();  // only copy data files
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Missing data files")
    void missingDataFiles() throws Exception {
        fileCopier.filterFiles("*_sig").copy();
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Less than 2/3 signatures")
    void lessThanTwoThirdSignatures() throws Exception {
        fileCopier.filterDirectories("*0.0.3").filterDirectories("*0.0.4").copy();
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Signature doesn't match file")
    void signatureMismatch() throws Exception {
        fileCopier.copy();
        Files.walk(s3Path).filter(p -> isSigFile(p.toString())).forEach(DownloaderTestingBase::corruptFile);
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Invalid or incomplete file")
    void invalidBalanceFile() throws Exception {
        fileCopier.copy();
        Files.walk(s3Path).filter(p -> isDataFile(p.toString())).forEach(DownloaderTestingBase::corruptFile);
        doReturn("").when(applicationStatusRepository).findByStatusCode(streamProperties.getLastValidDownloadedFileKey());
        doDownload();
        assertThat(Files.walk(validPath))
                .filteredOn(p -> !p.toFile().isDirectory())
                .hasSize(0);
    }

    @Test
    @DisplayName("Error moving record to valid folder")
    void errorMovingFile() throws Exception {
        fileCopier.copy();
        doReturn("").when(applicationStatusRepository).findByStatusCode(streamProperties.getLastValidDownloadedFileKey());
        validPath.toFile().delete();
        doDownload();
        assertThat(validPath).doesNotExist();
    }

    private static void corruptFile(Path p) {
        try {
            File file = p.toFile();
            if (file.isFile()) {
                FileUtils.writeStringToFile(file, "corrupt", "UTF-8", true);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

