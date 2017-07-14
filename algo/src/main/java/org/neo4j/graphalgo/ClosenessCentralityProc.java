package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphalgo.results.BetweennessCentralityProcResult;
import org.neo4j.graphalgo.results.ClosenessCentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ClosenessCentralityProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.closeness.stream")
    @Description("CALL algo.closeness.stream(label:String, relationship:String) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<MSClosenessCentrality.Result> closenessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        return new MSClosenessCentrality(graph, Pools.DEFAULT)
                .compute()
                .withLog(log)
                .resultStream();
    }

    @Procedure(value = "algo.closeness", mode = Mode.WRITE)
    @Description("CALL algo.closeness(label:String, relationship:String, {write:true, writeProperty:'centrality'}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes] - yields evaluation details")
    public Stream<ClosenessCentralityProcResult> closeness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final ClosenessCentralityProcResult.Builder builder = ClosenessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(Direction.OUTGOING)
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        final MSClosenessCentrality centrality = new MSClosenessCentrality(graph, Pools.DEFAULT)
                .withLog(log);

        builder.timeEval(centrality::compute);

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new ClosenessCentralityExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        graph,
                        configuration.getWriteProperty(),
                        Pools.DEFAULT)
                        .write(centrality.getCentrality());
            });
        }

        return Stream.of(builder.build());
    }
}
