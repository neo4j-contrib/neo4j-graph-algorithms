package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.DoubleArrayExporter;
import org.neo4j.graphalgo.impl.ClusteringCoefficient;
import org.neo4j.graphalgo.impl.TriangleCount;
import org.neo4j.graphalgo.results.ClusteringCoefficientResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ClusteringCoefficientProc {

    public static final String DEFAULT_WRITE_PROPERTY_VALUE = "clusterCoefficient";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.clusteringCoefficient.stream")
    @Description("CALL algo.clusteringCoefficient.stream(label, relationship, {concurrency:8}) " +
            "YIELD node, clusteringCoefficient")
    public Stream<ClusteringCoefficient.Result> triangleStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withLog(log)
                .withDirection(TriangleCount.D)
                .load(configuration.getGraphImpl());

        final ClusteringCoefficient clusteringCoefficient =
                new ClusteringCoefficient(graph, Pools.DEFAULT, configuration.getConcurrency())
                        .withProgressLogger(ProgressLogger.wrap(log, "triangleCoefficient"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction));

        return clusteringCoefficient
                .compute()
                .resultStream();
    }

    @Procedure(value = "algo.clusteringCoefficient", mode = Mode.WRITE)
    @Description("CALL algo.clusteringCoefficient(label, relationship, " +
            "{concurrency:8, write:true, writeProperty:'triangleCoefficient'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, coefficient")
    public Stream<ClusteringCoefficientResult> triangleCount(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final Graph graph;
        final ClusteringCoefficient triangleCount;
        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final ClusteringCoefficientResult.Builder builder = new ClusteringCoefficientResult.Builder();

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withLog(log)
                    .withDirection(TriangleCount.D)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = new ClusteringCoefficient(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
        };

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                new DoubleArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                        .write(triangleCount.getClusteringCoefficients());
            }
        }

        builder.withNodeCount(graph.nodeCount())
                .withCoefficient(triangleCount.getAverageClusteringCoefficient());

        return Stream.of(builder.build());
    }
}
