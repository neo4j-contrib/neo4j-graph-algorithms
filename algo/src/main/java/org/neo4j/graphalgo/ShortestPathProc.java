package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.ShortestPathDijkstra;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure("algo.shortestPath.stream")
    @Description("CALL algo.shortestPath.stream(startNodeId:long, endNodeId:long, propertyName:String" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
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
                .load(HeavyGraphFactory.class);

        return new ShortestPathDijkstra(graph)
                .compute(startNode.getId(), endNode.getId())
                .resultStream();
    }


    @Procedure("algo.shortestPath")
    @Description("CALL algo.shortestPath(startNode:Node, endNode:Node, propertyName:String" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost, loadDuration, evalDuration - yields nodeCount, totalCost, loadDuration, evalDuration")
    public Stream<ShortestPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        ShortestPathResult.Builder builder = ShortestPathResult.builder();

        final Graph graph;
        try (ProgressTimer timer = builder.load()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getPropertyDefaultValue(1.0))
                    .withExecutorService(Pools.DEFAULT)
                    .load(HeavyGraphFactory.class);
        };

        final ShortestPathDijkstra dijkstra;
        try (ProgressTimer timer = builder.eval()) {
            dijkstra = new ShortestPathDijkstra(graph)
                    .compute(startNode.getId(), endNode.getId());
        }

        builder.withNodeCount(dijkstra.getPathLength())
                .withTotalCosts(dijkstra.getTotalCost());

        return Stream.of(builder.build());
    }

}
