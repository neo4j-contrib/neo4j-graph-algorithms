package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.AtomicIntArrayExporter;
import org.neo4j.graphalgo.exporter.IntArrayExporter;
import org.neo4j.graphalgo.impl.ParallelTriangleCount;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class TriangleCountProc {

    public static final String DEFAULT_WRITE_PROPERTY_VALUE = "triangles";
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.triangles.stream")
    @Description("CALL algo.triangleCount({nodeQuery:'labelName', relationshipQuery:'relationshipName', concurrency:8}) " +
            "YIELD nodeId, triangles - yield nodeId, number of triangles")
    public Stream<ParallelTriangleCount.Result> triangleCountStream(@Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withLog(log)
                .withDirection(ParallelTriangleCount.D)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        final ParallelTriangleCount triangleCount = new ParallelTriangleCount(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        return triangleCount.resultStream();
    }

    @Procedure(value = "algo.triangles", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount({nodeQuery:'labelName', relationshipQuery:'relationshipName', concurrency:8, write:true, writeProperty:'triangles'}) " +
            "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount")
    public Stream<Result> triangleCount(@Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final Graph graph;
        final ParallelTriangleCount triangleCount;
        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final TriangleCountResultBuilder builder = new TriangleCountResultBuilder();

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withLog(log)
                    .withDirection(ParallelTriangleCount.D)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            triangleCount = new ParallelTriangleCount(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
        };

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                new AtomicIntArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_WRITE_PROPERTY_VALUE))
                        .write(triangleCount.getTriangles());
            }
        }

        builder.withNodeCount(graph.nodeCount())
                .withTriangleCount(triangleCount.getTriangleCount());

        return Stream.of(builder.build());
    }

    /**
     * result dto
     */
    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long nodeCount;
        public final long triangleCount;

        public Result(long loadMillis, long computeMillis, long writeMillis, long nodeCount, long triangleCount) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodeCount = nodeCount;
            this.triangleCount = triangleCount;
        }
    }

    public class TriangleCountResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = -1L;
        private long triangleCount = -1L;

        public TriangleCountResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public TriangleCountResultBuilder withTriangleCount(long triangleCount) {
            this.triangleCount = triangleCount;
            return this;
        }

        @Override
        public Result build() {
            return new Result(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    triangleCount);
        }
    }

}
