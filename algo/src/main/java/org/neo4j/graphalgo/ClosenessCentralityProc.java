package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphalgo.exporter.DoubleArrayExporter;
import org.neo4j.graphalgo.results.ClosenessCentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ClosenessCentralityProc {


    public static final String DEFAULT_TARGET_PROPERTY = "centrality";


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.closeness.stream")
    @Description("CALL algo.closeness.stream(label:String, relationship:String{concurrency:4}) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<MSClosenessCentrality.Result> closenessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        final MSClosenessCentrality algo = new MSClosenessCentrality(graph, configuration.getConcurrency(), Pools.DEFAULT)
                .withProgressLogger(ProgressLogger.wrap(log, "ClosenessCentrality(MultiSource)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();
        graph.release();
        return algo.resultStream();
    }

    @Procedure(value = "algo.closeness", mode = Mode.WRITE)
    @Description("CALL algo.closeness(label:String, relationship:String, {write:true, writeProperty:'centrality, concurrency:4'}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes] - yields evaluation details")
    public Stream<ClosenessCentralityProcResult> closeness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final ClosenessCentralityProcResult.Builder builder = ClosenessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(Direction.OUTGOING)
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        final MSClosenessCentrality centrality = new MSClosenessCentrality(graph, configuration.getConcurrency(), Pools.DEFAULT)
                .withProgressLogger(ProgressLogger.wrap(log, "ClosenessCentrality(MultiSource)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(centrality::compute);

        if (configuration.isWriteFlag()) {

            final double[] centralityResult = centrality.getCentrality();
            centrality.release();
            graph.release();
            builder.timeWrite(() -> {
                new DoubleArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(centralityResult);
            });
        }

        return Stream.of(builder.build());
    }
}
