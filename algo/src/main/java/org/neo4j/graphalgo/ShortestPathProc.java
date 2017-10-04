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

    /**
     * single threaded dijkstra impl.
     * takes a startNode and endNode id and tries to find the best path
     * supports direction flag in configuration ( see {@link org.neo4j.graphalgo.core.utils.Directions})
     * default is: BOTH
     *
     * @param startNode
     * @param endNode
     * @param propertyName
     * @param config
     * @return
     */
    @Procedure("algo.shortestPath.stream")
    @Description("CALL algo.shortestPath.stream(startNode:Node, endNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0}) " +
            "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Direction direction = configuration.getDirection(Direction.BOTH);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(direction)
                .load(configuration.getGraphImpl());

        return new ShortestPathDijkstra(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(startNode.getId(), endNode.getId(), direction)
                .resultStream();
    }

    @Procedure(value = "algo.shortestPath", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath(startNode:Node, endNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', dirction:'BOTH', defaultValue:1.0, write:'true', writeProperty:'sssp'}) " +
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

        final Direction direction = configuration.getDirection(Direction.BOTH);
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getPropertyDefaultValue(1.0))
                    .withDirection(direction)
                    .load(configuration.getGraphImpl());
        }

        try (ProgressTimer timer = builder.timeEval()) {
            dijkstra = new ShortestPathDijkstra(graph)
                    .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(startNode.getId(), endNode.getId(), direction);
            builder.withNodeCount(dijkstra.getPathLength())
                    .withTotalCosts(dijkstra.getTotalCost());
        }

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
