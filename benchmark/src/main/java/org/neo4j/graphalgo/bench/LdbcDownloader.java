package org.neo4j.graphalgo.bench;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

public final class LdbcDownloader {

    private static Path DEFAULT_TEMP_DIR;
    private static final String GRAPH_FILE_NAME = "ldbc_sf001_p006_neo4j31.tgz";
    private static final String GRAPH_LOCATION = "http://example-data.neo4j.org.s3.amazonaws.com/files/ldbc_sf001_p006_neo4j31.tgz";
    private static final URL GRAPH_URL;
    static {
        try {
            GRAPH_URL = new URL(GRAPH_LOCATION);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    static synchronized GraphDatabaseAPI openDb() throws IOException {
        Path graphDir = tempDirFor("org.neo4j", "ldbc");
        Path graphDbDir = graphDir.resolve("graph.db");
        if (Files.isDirectory(graphDbDir)) {
            return openDb(graphDbDir);
        }
        Path zippedDb = graphDir.resolve(GRAPH_FILE_NAME);
        if (Files.isReadable(zippedDb)) {
            unzipFile(zippedDb);
            return openDb();
        }
        downloadFile(zippedDb);
        return openDb();
    }

    private static GraphDatabaseAPI openDb(Path dbLocation) {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbLocation.toFile())
                .newGraphDatabase();
        return (GraphDatabaseAPI) db;
    }

    public static void main(final String... args) throws Exception {
        GraphDatabaseAPI db = openDb();
        System.out.println("db = " + db);
        db.shutdown();
    }

    private static void unzipFile(Path zippedDb)
    throws IOException {
        Path tarFile = unGzip(zippedDb);
        unTar(tarFile, zippedDb.getParent());
        Files.deleteIfExists(tarFile);
    }

    private static void downloadFile(final Path target)
    throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection) GRAPH_URL.openConnection();

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            try (InputStream in = connection.getInputStream();
                 OutputStream out = Files.newOutputStream(target)) {
                IOUtils.copy(in, out);
            }
        } else {
            throw new IOException("Invalid S3 response: " + responseCode);
        }
    }

    private static Path tempDirFor(final String... subDirs) throws IOException {
        Path tmpDir = getDefaultTempDir().toAbsolutePath();
        for (String subDir : subDirs) {
            tmpDir = tmpDir.resolve(subDir);
            createDir(tmpDir);
        }
        if (!Files.isWritable(tmpDir)) {
            throw new IOException("Temporary folder at [" + tmpDir + "] is not writable");
        }
        return tmpDir;
    }

    private static void createDir(final Path tmpDir) throws IOException {
        try {
            Files.createDirectory(tmpDir);
        } catch (FileAlreadyExistsException faee) {
            // we want a tmp dir, if it's there, fine, if it's a file, throw
            if (!Files.isDirectory(tmpDir)) {
                throw faee;
            }
        }
    }

    private static synchronized Path getDefaultTempDir() throws IOException {
        if (DEFAULT_TEMP_DIR == null) {
            // Lazy init
            final String tempDirPath = System.getProperty("java.io.tmpdir");
            if (tempDirPath == null) {
                throw new IOException(
                        "Java has no temporary folder property (java.io.tmpdir)?");
            }
            final Path tempDirectory = Paths.get(tempDirPath);
            if (!Files.isWritable(tempDirectory)) {
                throw new IOException(
                        "Java's temporary folder not present or writeable?: "
                                + tempDirectory.toAbsolutePath());
            }
            DEFAULT_TEMP_DIR = tempDirectory;
        }

        return DEFAULT_TEMP_DIR;
    }

    private static void unTar(final Path inputFile, final Path targetDir)
    throws IOException {
        try (InputStream in = Files.newInputStream(inputFile);
        TarArchiveInputStream tar = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR, in)) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextTarEntry()) != null) {
                Path outFile = targetDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    if (!Files.isDirectory(outFile)) {
                        Files.createDirectory(outFile);
                    }
                } else {
                    try (OutputStream out = Files.newOutputStream(outFile)) {
                        IOUtils.copy(tar, out);
                    }
                }
            }
        } catch (ArchiveException e) {
            throw new IOException(e);
        }
    }

    private static Path unGzip(final Path inputFile) throws IOException {
        String fileName = inputFile.getFileName().toString();
        assert fileName.endsWith(".tgz");
        Path targetFile = inputFile
                .getParent()
                .resolve(fileName.replace(".tgz", ".tar"));

        try (InputStream in = Files.newInputStream(inputFile);
             GZIPInputStream gzipIn = new GZIPInputStream(in);
             OutputStream out = Files.newOutputStream(targetFile)) {
            IOUtils.copy(gzipIn, out);
        }

        return targetFile;
    }
}
