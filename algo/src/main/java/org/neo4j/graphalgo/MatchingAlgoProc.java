package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.MatchingAlgo;
import org.neo4j.graphalgo.results.MatchingAlgoResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public class MatchingAlgoProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "fk.algo.matching_algo_capacity", mode = Mode.READ)
    @Description("CALL fk.algo.matching_algo(capacityNode:Node, totalDistancePropertyName:String, " +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName'}) " +
            "YIELD loadUuid, offRoute, deadHead, loadMillis, evalMillis  - yields loadUuid, offRoute, deadHead, loadMillis, evalMillis")
    public Stream<MatchingAlgoResult> matchingAlgoCapacity(
            @Name("capacityNode") Node capacityNode,
            @Name("originCityNode") Node originCityNode,
            @Name("destinationCityNode") Node destCityNode,
            @Name("cityRouteRelation") String cityRouteRelation,
            @Name("totalDistancePropertyName") String totalDistancePropertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final MatchingAlgoResult.Builder builder = new MatchingAlgoResult.Builder();
        // load
//        try (ProgressTimer timer = builder.timeLoad()) {
//            final GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
//                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration);
//            // use undirected traversal if direction is BOTH
//            if (direction == Direction.BOTH) {
//                direction = Direction.OUTGOING; // rewrite
//                graphLoader.asUndirected(true);
//            } else {
//                graphLoader.withDirection(direction);
//            }
//            graph = graphLoader.load(configuration.getGraphImpl());
//        }
//
//        if(graph.nodeCount() == 0 || capacityNode == null){
//            graph.release();
//            return Stream.empty();
//        }
        final MatchingAlgo matchingAlgo;

        try (ProgressTimer timer = builder.timeEval()) {
            matchingAlgo = new MatchingAlgo(capacityNode, originCityNode, destCityNode, cityRouteRelation, configuration, api)
                    .withProgressLogger(ProgressLogger.wrap(log, "MatchingAlgo"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
        }


        return Stream.of(builder.build());
    }
}
