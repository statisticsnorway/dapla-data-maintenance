package no.ssb.dapla.datamaintenance.storage;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
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
    private static final String DRYRUN_MARKER = ".DRYRUNDELETE";

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
            return URI.create(Path.of(path.getPath()).normalize().toString());
        } else {
            return path;
        }
    }

    // We might need to cache the file system by bucket with token expiration as eviction?
    StorageOptions getStorageOptions(Credentials credentials) throws IOException {
        return StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build();
    }

    FileSystem setupFileSystem(URI prefix, Credentials credentials) throws IOException {
        var scheme = prefix.getScheme();
        if (GCS_SCHEME.equalsIgnoreCase(scheme)) {
            var bucketName = prefix.getHost();
            return CloudStorageFileSystem.forBucket(
                    bucketName,
                    // CloudStorageConfiguration.DEFAULT
                    CloudStorageConfiguration.builder()
                        .autoDetectRequesterPays(false)
                        .userProject(null)
                        .build(),
                    getStorageOptions(credentials)
            );
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
     * @param dryRun dry run flag.
     * @param prefix the prefix to delete.
     */
    public Single<Path> markDelete(URI prefix, Credentials credentials, Boolean dryRun) {
        return mark(prefix, credentials, dryRun ? DRYRUN_MARKER : DELETED_MARKER);
    }

    private Single<Path> mark(URI prefix, Credentials credentials, String markerName) {
        return Single.defer(() -> {
            try {
                var fileSystem = setupFileSystem(prefix, credentials);
                var file = removeSchemeAndHost(prefix);
                var path = fileSystem.getPath(file.getPath());
                try {
                    // TODO: Append info about who tried to delete.
                    Path markerPath = path.resolve(markerName);
                    if (!Files.exists(markerPath)) {
                        Files.createFile(markerPath);
                    }
                    return Single.just(markerPath);
                } catch (FileAlreadyExistsException faee) {
                    // TODO: Log
                    return Single.empty();
                }
            } catch (IOException ioe) {
                return Single.error(ioe);
            }
        });
    }

    Multi<Path> findMarked(URI prefix, Credentials credentials, String markerName) {
        return Multi.defer(() -> {
            try {
                var fileSystem = setupFileSystem(prefix, credentials);
                var path = fileSystem.getPath(removeSchemeAndHost(prefix).getPath());

                // TODO: check if a recursive implementation with Files.find() and a folder
                //  filter would improve performances. I suspect that GCS under the hood need
                //  to go through the files sequentially anyways.
                return Multi.concat(
                        Multi.just(path),
                        Multi.create(Files.list(path))
                ).filter(subDirectory -> {
                    return Files.isDirectory(subDirectory) && Files.exists(subDirectory.resolve(markerName));
                });
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
    public Multi<PathAndSize> finishDelete(URI prefix, Credentials credentials, Boolean dryRun) {
        Multi<Path> markedPaths = findMarked(prefix, credentials, dryRun ? DRYRUN_MARKER : DELETED_MARKER);
        if (dryRun) {
            return markedPaths.flatMap(path -> findAsync(path, Integer.MAX_VALUE, (foundPath, attr) -> !attr.isDirectory()))
                    .flatMap(path -> sizeAsync(path).map(size -> new PathAndSize(path, size)))
                    .flatMap(pathAndSize -> {
                        // delete the dry run marker.
                        if (pathAndSize.path().endsWith(DRYRUN_MARKER)) {
                            return deleteIfExistsAsync(pathAndSize.path()).map(path -> pathAndSize);
                        } else {
                            return Single.just(pathAndSize);
                        }
                    });
        } else {
            return markedPaths.flatMap(pathToDelete -> deleteAllUnder(pathToDelete));
        }

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
