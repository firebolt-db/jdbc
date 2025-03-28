package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileService {

    private DirectoryPathResolver directoryPathResolver;
    private FilenameGenerator filenameGenerator;
    private ExecutorService executorService;

    public FileService() {
        this(new DirectoryPathResolver(), new FilenameGenerator(), Executors.newFixedThreadPool(2));
    }

    // visible for testing
    FileService(DirectoryPathResolver directoryPathResolver, FilenameGenerator filenameGenerator, ExecutorService executorService) {
        this.directoryPathResolver = directoryPathResolver;
        this.filenameGenerator = filenameGenerator;
        this.executorService = executorService;
    }

    /**
     * Returns true if the file exists. False otherwise
     *
     * @param cacheKey - the key for which we are looking for the file
     * @return
     */
    public Optional<File> findFileForKey(CacheKey cacheKey) {
        // generate the file name
        Optional<String> filenameOptional = filenameGenerator.generate(cacheKey);

        // cannot generate the file is due to encryption most likely
        if (filenameOptional.isEmpty()) {
            return Optional.empty();
        }

        String filenameForCache = filenameOptional.get();
        Path fireboltJdbcDriverFolder = directoryPathResolver.resolveFireboltJdbcDirectory();

        File file = new File(Paths.get(fireboltJdbcDriverFolder.toString(), filenameForCache).toString());
        return Optional.of(file);
    }

    public void safeSaveToDiskAsync(CacheKey cacheKey, ConnectionCache connectionCache) {
        executorService.submit(() -> {
            // prepare the object to save
            OnDiskConnectionCache onDiskConnectionCache =

            // check if a file exists

        });
    }
}
