package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.ShortestPathDijkstra;
import org.neo4j.graphalgo.results.DijkstraResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathProc {

    public static final String DEFAULT_TARGET_PROPERTY = "sssp";


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.shortestPath.stream")
    @Description("CALL algo.shortestPath.stream(startNodeId:long, endNodeId:long, propertyName:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withLog(log)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        return new ShortestPathDijkstra(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(startNode.getId(), endNode.getId())
                .resultStream();
    }

    @Procedure(value = "algo.shortestPath", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath(startNodeId:long, endNodeId:long, propertyName:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost, loadMillis, evalMillis, writeMillis - yields nodeCount, totalCost, loadMillis, evalMillis, writeMillis")
    public Stream<DijkstraResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        DijkstraResult.Builder builder = DijkstraResult.builder();

        final Graph graph;
        final ShortestPathDijkstra dijkstra;

        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withLog(log)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getPropertyDefaultValue(1.0))
                    .withDirection(Direction.OUTGOING)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        };

        try (ProgressTimer timer = builder.timeEval()) {
            dijkstra = new ShortestPathDijkstra(graph)
                    .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(startNode.getId(), endNode.getId());
            builder.withNodeCount(dijkstra.getPathLength())
                    .withTotalCosts(dijkstra.getTotalCost());
        };

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                final IntArrayDeque finalPath = dijkstra.getFinalPath();
                dijkstra.release();
                new ShortestPathDijkstra.SPExporter(graph, api, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY))
                        .write(finalPath);
            }
        }

        return Stream.of(builder.build());
    }

}
