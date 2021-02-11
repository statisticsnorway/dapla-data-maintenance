package no.ssb.dapla.datamaintenance.storage;

import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.CloudStorageConfiguration;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

import java.io.InputStream;
import java.nio.file.FileSystem;
import java.util.concurrent.Executor;

public class TestableStorageService extends StorageService {

    private static final StorageOptions STORAGE_OPTIONS = LocalStorageHelper.getOptions();

    public TestableStorageService(Executor executor) {
        super(executor);
    }

    public TestableStorageService() {
    }

    @Override
    StorageOptions getStorageOptions(InputStream token) {
        return STORAGE_OPTIONS;
    }

    public FileSystem getFileSystem(String bucketName) {
        return CloudStorageFileSystem.forBucket(
                bucketName,
                CloudStorageConfiguration.DEFAULT,
                getStorageOptions(null)
        );
    }
}
