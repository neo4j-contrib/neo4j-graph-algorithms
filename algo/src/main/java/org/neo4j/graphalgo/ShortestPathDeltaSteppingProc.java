package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.DeltaSteppingShortestPathExporter;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphalgo.results.DeltaSteppingProcResult;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 *
 * More information in:<br>
 *
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 *
 * @author mknblch
 */
public class ShortestPathDeltaSteppingProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure("algo.deltaStepping.stream")
    @Description("CALL algo.deltaStepping.stream(startNode:Node, propertyName:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, distance} from start to end (inclusive)")
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> deltaSteppingStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(Double.MAX_VALUE))
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        return new ShortestPathDeltaStepping(graph, delta)
                .withExecutorService(Executors.newFixedThreadPool(
                        configuration.getInt("concurrency", 4)
                ))
                .compute(startNode.getId())
                .resultStream();
    }

    @Procedure(value = "algo.deltaStepping", mode = Mode.WRITE)
    @Description("CALL algo.deltaStepping(startNode:Node, propertyName:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0, write:true, writeProperty:'sssp'}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount")
    public Stream<DeltaSteppingProcResult> deltaStepping(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final DeltaSteppingProcResult.Builder builder = DeltaSteppingProcResult.builder();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getPropertyDefaultValue(Double.MAX_VALUE))
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        }

        final ShortestPathDeltaStepping algorithm = new ShortestPathDeltaStepping(graph, delta)
                .withExecutorService(Pools.DEFAULT);

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new DeltaSteppingShortestPathExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        new NodeBatch(graph.nodeCount()),
                        configuration.get(WRITE_PROPERTY, DEFAULT_TARGET_PROPERTY),
                        Pools.DEFAULT).write(algorithm.getShortestPaths());
            });
        }

        return Stream.of(builder
                .withNodeCount(graph.nodeCount())
                .build());
    }

    private final static class NodeBatch implements BatchNodeIterable {

        public final int nodeCount;

        private NodeBatch(int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
            int numberOfBatches = ParallelUtil.threadSize(batchSize, nodeCount);
            ArrayList<PrimitiveIntIterable> result = new ArrayList<>();
            for (int i = 0; i < numberOfBatches; i += batchSize) {
                int end = i + batchSize > nodeCount ? nodeCount : i + batchSize;
                result.add(new BatchedNodeIterator(i, end));
            }
            return result;
        }

    }

    private static class BatchedNodeIterator implements PrimitiveIntIterator, PrimitiveIntIterable {

        private final int end;
        private int current;

        private BatchedNodeIterator(int start, int end) {
            this.end = end;
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return current < end;
        }

        @Override
        public int next() {
            return current++;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return this;
        }
    }
}
