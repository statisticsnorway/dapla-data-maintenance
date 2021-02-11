package no.ssb.dapla.datamaintenance.storage;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.CloudStorageConfiguration;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

/**
 * Two phase delete on object store.
 */
@ApplicationScoped
public class StorageService {

    private static final String GCS_SCHEME = "gs";
    private static final String FILE_SCHEME = "file";
    private static final String DELETED_MARKER = ".DELETED";

    private Executor executor = null;

    public StorageService(Executor executor) {
        this.executor = executor;
    }

    public StorageService() {
    }

    static URI removeSchemeAndHost(URI path) {
        var scheme = path.getScheme();
        if (GCS_SCHEME.equalsIgnoreCase(scheme)) {
            // Strip the scheme and bucket name for GCS.
            return URI.create(path.getPath());
        } else {
            return path;
        }
    }

    // We might need to cache the file system by bucket with token expiration as eviction?
    StorageOptions getStorageOptions(InputStream token) throws IOException {
        return StorageOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(token))
                .build();
    }

    FileSystem setupFileSystem(URI prefix, InputStream token) throws IOException {
        var scheme = prefix.getScheme();
        if (GCS_SCHEME.equalsIgnoreCase(scheme)) {
            var bucketName = prefix.getHost();
            return CloudStorageFileSystem.forBucket(bucketName, CloudStorageConfiguration.DEFAULT, getStorageOptions(token));
        } else if (FILE_SCHEME.equalsIgnoreCase(scheme)) {
            return FileSystems.getDefault();
        } else {
            throw new UnsupportedOperationException("scheme " + scheme + " is not supported");
        }
    }

    Executor getExecutor() {
        return executor != null ? executor : ForkJoinPool.commonPool();
    }

    /**
     * Mark the prefix to be deleted
     *
     * @param prefix the prefix to delete.
     */
    public Single<Path> markDelete(URI prefix, InputStream token) {
        return Single.defer(() -> {
            try {
                var fileSystem = setupFileSystem(prefix, token);
                var file = removeSchemeAndHost(prefix);
                var path = fileSystem.getPath(file.getPath());
                try {
                    // TODO: Append info about who tried to delete.
                    Files.createFile(path.resolve(DELETED_MARKER));
                } catch (FileAlreadyExistsException faee) {
                    // TODO: Log
                    return Single.empty();
                }
                return Single.just(path);
            } catch (IOException ioe) {
                return Single.error(ioe);
            }
        });
    }

    /**
     * Delete the files marked for deletion
     *
     * @param prefix the prefix
     * @return the list of files that are deleted.
     */
    public Multi<PathAndSize> finishDelete(URI prefix, InputStream token) {
        return Multi.defer(() -> {
            try {
                var fileSystem = setupFileSystem(prefix, token);
                var path = fileSystem.getPath(removeSchemeAndHost(prefix).getPath());

                // TODO: check if a recursive implementation with Files.find() and a folder
                //  filter would improve performances. I suspect that GCS under the hood need
                //  to go through the files sequentially anyways.
                return Multi.concat(
                        Multi.just(path),
                        Multi.create(Files.list(path))
                ).filter(subDirectory -> {
                    return Files.isDirectory(subDirectory) && Files.exists(subDirectory.resolve(DELETED_MARKER));
                }).flatMap(pathToDelete -> deleteAllUnder(pathToDelete));
            } catch (IOException ioe) {
                return Single.error(ioe);
            }
        });
    }

    /**
     * Like {@link Files#size(Path)} but async.
     */
    public Single<Long> sizeAsync(Path path) {
        return Single.defer(() -> {
            try {
                return Single.just(Files.size(path));
            } catch (IOException e) {
                return Single.error(e);
            }
        }).observeOn(getExecutor());
    }

    /**
     * Delete the files marked for deletion
     *
     * @param prefix the prefix
     * @return the list of files that are deleted.
     */
    private Multi<PathAndSize> deleteAllUnder(Path prefix) {
        return findAsync(prefix, Integer.MAX_VALUE, (foundPath, attr) -> !attr.isDirectory())
                .flatMap(pathToDelete ->
                        Single.create(
                                sizeAsync(pathToDelete).onErrorResume(throwable -> -1L)
                                        .thenCombine(deleteIfExistsAsync(pathToDelete), (size, path) ->
                                                new PathAndSize(path, size)
                                        ))
                );
    }

    /**
     * Like {@link Files#find} but async.
     */
    private Multi<Path> findAsync(Path start, int maxDepth, BiPredicate<Path, BasicFileAttributes> matcher,
                                  FileVisitOption... options) {
        return Single.defer(() -> {
            try {
                return Single.just(Files.find(start, maxDepth, matcher, options));
            } catch (IOException ioe) {
                return Single.error(ioe);
            }
        }).observeOn(getExecutor()).flatMap(Multi::create);
    }

    /**
     * Like {@link Files#deleteIfExists(Path)} but async.
     */
    private Single<Path> deleteIfExistsAsync(Path path) {
        return Single.defer(() -> {
            try {
                if (Files.deleteIfExists(path)) {
                    return Single.just(path);
                } else {
                    return Single.empty();
                }
            } catch (IOException ioe) {
                return Single.error(ioe);
            }
        }).observeOn(getExecutor());
    }
}
