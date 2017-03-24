package org.neo4j.graphalgo.serialize;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFileLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFileWriter;
import org.neo4j.graphalgo.core.leightweight.LightGraph;
import org.neo4j.graphalgo.core.leightweight.LightGraphFileLoader;
import org.neo4j.graphalgo.core.leightweight.LightGraphFileWriter;

import java.io.IOException;
import java.nio.file.Path;

public final class FileSerialization {

    public static void writeGraph(Graph graph, Path dbPath)
    throws IOException {
        if (graph instanceof LightGraph) {
            LightGraphFileWriter.serialize((LightGraph) graph, dbPath);
        }
        else if (graph instanceof HeavyGraph) {
            HeavyGraphFileWriter.serialize((HeavyGraph) graph, dbPath);
        }
    }

    public static Graph loadLightGraph(Path dbPath)
    throws IOException {
        return LightGraphFileLoader.load(dbPath);
    }

    public static Graph loadHeavyGraph(Path dbPath)
    throws IOException {
        return HeavyGraphFileLoader.load(dbPath);
    }

    private FileSerialization() {
        throw new UnsupportedOperationException("No instances");
    }
}
