package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.ShortestPathDijkstra;
import org.neo4j.graphalgo.results.DijkstraResult;
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
public class DijkstraProc {

    public static final String CONFIG_LABEL = "label";
    public static final String CONFIG_RELATIONSHIP = "relationship";
    public static final String CONFIG_DEFAULT_VALUE = "defaultValue";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure("algo.dijkstraStream")
    @Description("CALL algo.dijkstraStream(startNodeId:long, endNodeId:long, propertyName:String" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel((String) config.get(CONFIG_LABEL))
                .withOptionalRelationshipType((String) config.get(CONFIG_RELATIONSHIP))
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        (double) config.getOrDefault(CONFIG_DEFAULT_VALUE, 1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        return new ShortestPathDijkstra(graph)
                .compute(startNode.getId(), endNode.getId())
                .resultStream();
    }


    @Procedure("algo.dijkstra")
    @Description("CALL algo.dijkstra(startNodeId:long, endNodeId:long, propertyName:String" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, cost, loadDuration, evalDuration - yields nodeCount, totalCost, loadDuration, evalDuration")
    public Stream<DijkstraResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        DijkstraResult.Builder builder = DijkstraResult.builder();

        ProgressTimer load = builder.load();
        final Graph graph = new GraphLoader(api)
                .withOptionalLabel((String) config.get(CONFIG_LABEL))
                .withOptionalRelationshipType((String) config.get(CONFIG_RELATIONSHIP))
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        (double) config.getOrDefault(CONFIG_DEFAULT_VALUE, 1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
        load.stop();

        ProgressTimer eval = builder.eval();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(startNode.getId(), endNode.getId());
        eval.stop();

        builder.withNodeCount(dijkstra.getPathLength())
                .withTotalCosts(dijkstra.getTotalCost());

        return Stream.of(builder.build());
    }

}
