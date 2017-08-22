package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.ParallelTriangleList;
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
public class TriangleListProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.triangles.stream")
    @Description("CALL algo.triangleCount({nodeQuery:'labelName', relationshipQuery:'relationshipName', concurrency:8}) " +
            "YIELD loadMillis, evalMillis, nodeCount, triangleCount - yields results")
    public Stream<ParallelTriangleList.Result> triangleListStream(@Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {


        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .withDirection(ParallelTriangleList.D)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        final ParallelTriangleList algorithm = new ParallelTriangleList(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "triangleList"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        return algorithm.resultStream();
    }

    @Procedure(value = "algo.triangles", mode = Mode.WRITE)
    @Description("CALL algo.triangleCount({nodeQuery:'labelName', relationshipQuery:'relationshipName', concurrency:8, write:true}) " +
            "YIELD loadMillis, evalMillis, nodeCount, triangleCount - yields results")
    public Stream<Result> triangleList(@Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final TriangleListResultBuilder builder = new TriangleListResultBuilder();
        final ParallelTriangleList algorithm;
        final Graph graph;

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withDirection(ParallelTriangleList.D)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            algorithm = new ParallelTriangleList(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "triangleList"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
        };

        builder.withNodeCount(graph.nodeCount());

        return null;
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

    public class TriangleListResultBuilder extends AbstractResultBuilder<Result> {

        private long nodeCount = -1L;
        private long triangleCount = -1L;

        public TriangleListResultBuilder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public TriangleListResultBuilder withTriangleCount(long triangleCount) {
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
