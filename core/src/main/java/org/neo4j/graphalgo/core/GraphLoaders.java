package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFileLoader;
import org.neo4j.graphalgo.core.leightweight.LowLevelFileLoader;
import org.neo4j.graphalgo.core.leightweight.LowLevelNeo4jLoader;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.nio.file.Path;

public final class GraphLoaders {

    public static WeightedGraph serializedLowLevel(Path dbPath)
    throws IOException {
        return LowLevelFileLoader.load(dbPath);
    }

    public static WeightedGraph serializedMultiArray(Path dbPath)
    throws IOException {
        return HeavyGraphFileLoader.load(dbPath);
    }

    public static WeightedGraph lowLevel(GraphDatabaseService graphDatabaseService) {
        return lowLevel(graphDatabaseService, null);
    }

    public static WeightedGraph lowLevel(
            GraphDatabaseService graphDatabaseService,
            String relationshipWeightProperty) {
        return new LowLevelNeo4jLoader().load(
                graphDatabaseService,
                relationshipWeightProperty);
    }

    private GraphLoaders() {
        throw new UnsupportedOperationException("No instances");
    }
}
