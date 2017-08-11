package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public final class LoadProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    public static class GraphResult {
        public final Object graph;
        public final long nodes;
        public final long loadMillis;

        public GraphResult(Object graph, long nodes, long loadMillis) {
            this.graph = graph;
            this.nodes = nodes;
            this.loadMillis = loadMillis;
        }
    }
    @Procedure(value = "algo.load", mode = Mode.WRITE)
    @Description("CALL algo.load(label:String, relationship:String, {graph:'heavy/light/cypher',direction:'OUTGOING'," +
            " nodeValue:'value', defaultNodeValue:0.0,nodeWeight:'weight',defaultNodeWeight:0.0, relationshipWeight:'cost', defaultRelationshipWeight:1.0" +
            " }) YIELD graph, nodes, loadMillis  - loads graph and returns the graph object")
    public Stream<GraphResult> loadGraph(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Stats.Builder statsBuilder = new Stats.Builder();

        GraphLoader graphLoader = new GraphLoader(api)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withOptionalNodeProperty((String)config.get("nodeValue"), (Double)config.getOrDefault("defaultNodeValue",1d))
                .withOptionalNodeWeightsFromProperty((String)config.get("nodeWeight"), (Double)config.getOrDefault("defaultNodeWeight",1d))
                .withOptionalRelationshipWeightsFromProperty((String)config.get("relationshipWeight"), (Double)config.getOrDefault("defaultRelationshipWeight",1d))
                .withDirection(Direction.valueOf(configuration.get(ProcedureConstants.DIRECTION, "OUTGOING")))
                .withExecutorService(Pools.DEFAULT)
                .dontRelease();

        Graph graph;
        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            graph = graphLoader.load(configuration.getGraphImpl());
            statsBuilder.withNodes(graph.nodeCount());
//            statsBuilder.withRelationships(graph.relationshipCount());
        }
        Stats stats = statsBuilder.build();
        return Stream.of(new GraphResult(graph,stats.nodes,stats.loadMillis));
    }

    public static final class Stats {
        public final long nodes, relationships, loadMillis;

        Stats(long nodes, long relationships, long loadMillis) {
            this.nodes = nodes;
            this.relationships = relationships;
            this.loadMillis = loadMillis;
        }

        public static final class Builder extends AbstractResultBuilder<Stats> {
            private long nodes;
            private long relationships;

            public Builder withNodes(long nodes) {
                this.nodes = nodes;
                return this;
            }

            public Builder withRelationships(long relationships) {
                this.relationships = relationships;
                return this;
            }

            public Stats build() {
                return new Stats(nodes, relationships, loadDuration);
            }
        }
    }

}
