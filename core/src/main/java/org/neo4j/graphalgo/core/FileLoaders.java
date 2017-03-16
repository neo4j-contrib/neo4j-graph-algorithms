package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFileLoader;
import org.neo4j.graphalgo.core.leightweight.LowLevelFileLoader;

import java.io.IOException;
import java.nio.file.Path;

public final class FileLoaders {

    public static Graph serializedLowLevel(Path dbPath)
    throws IOException {
        return LowLevelFileLoader.load(dbPath);
    }

    public static Graph serializedMultiArray(Path dbPath)
    throws IOException {
        return HeavyGraphFileLoader.load(dbPath);
    }

    private FileLoaders() {
        throw new UnsupportedOperationException("No instances");
    }
}
