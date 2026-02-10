package com.huawei.falcon.state.cache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * This class is used to load the Falcon shared library from flink jars. The shared library will be extracted to a temp
 * folder and then loaded from there.
 */
public class FalconLibLoader {
    private static final FalconLibLoader instance = new FalconLibLoader();
    private static boolean initialized = false;

    /**
     * Get a reference to the FalconLibLoader
     *
     * @return The FalconLibLoader
     */
    public static FalconLibLoader getInstance() {
        return instance;
    }

    /**
     * Attempts to load falcon library from inside jar. In detail, files inside jar will be copied to a temporary
     * directory, then falcon library will be loaded from such temporary directory. Note that temp directory is
     * "java.io.tmpdir" by default.
     * @throws IOException if a filesystem operation fails.
     */
    public synchronized void loadLibrary() throws IOException {
        try {
            loadLibraryFromJar();
        } catch (IOException e) {
            throw new RuntimeException("[FALCON] falcon dynamic library is not successfully loaded.", e);
        }
    }

    /**
     * Attempts to extract the native Falcon library from inside jar to "java.io.tmpdir", and then load it
     *
     * @throws java.io.IOException if a filesystem operation fails.
     */
    void loadLibraryFromJar() throws IOException {
        if (!initialized) {
            System.load(loadLibraryFromJarToTemp().getAbsolutePath());  // load library using absolute path
            initialized = true;
        }
    }

    /**
     * Extract the native Falcon library from inside jar to "java.io.tmpdir"
     *
     * @throws IOException if a filesystem operation fails.
     */
    File loadLibraryFromJarToTemp() throws IOException {
        // create an empty file in "java.io.tmpdir" named libfalcon.so
        final File temp = File.createTempFile("libfalcon", ".so");
        if (!temp.exists()) {
            throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
        } else {
            temp.deleteOnExit();
        }

        // attempt to copy the library from the Jar file to the temp destination
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("libfalcon.so")) {
            if (is == null) {
                throw new RuntimeException("[FALCON] libfalcon.so was not found inside JAR.");
            } else {
                Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        return temp;
    }

    /**
     * Private constructor to disallow instantiation
     */
    private FalconLibLoader() {
    }
}
