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
import org.neo4j.graphalgo.impl.ShortestPaths;
import org.neo4j.graphalgo.impl.ShortestPathsExporter;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathsProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure("algo.shortestPaths.stream")
    @Description("CALL algo.shortestPaths.stream(startNodeId:long, propertyName:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPaths.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        return new ShortestPaths(graph)
                .compute(startNode.getId())
                .resultStream();
    }

    @Procedure(value = "algo.shortestPaths", mode = Mode.WRITE)
    @Description("CALL algo.shortestPaths(startNodeId:long, propertyName:String" +
            "{write:true, targetProperty:'path', nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount, targetProperty - yields nodeCount, totalCost, loadDuration, evalDuration")
    public Stream<ShortestPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        ShortestPathResult.Builder builder = ShortestPathResult.builder();

        ProgressTimer load = builder.timeLoad();
        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());
        load.stop();

        ProgressTimer eval = builder.timeEval();
        final ShortestPaths algorithm = new ShortestPaths(graph)
                .compute(startNode.getId());
        eval.stop();

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new ShortestPathsExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        new NodeBatch(graph.nodeCount()),
                        configuration.get(WRITE_PROPERTY, DEFAULT_TARGET_PROPERTY),
                        Pools.DEFAULT).write(algorithm.getShortestPaths());
            });
        }

        return Stream.of(builder.build());
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
                result.add(new ShortestPathsProc.BatchedNodeIterator(i, end));
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
