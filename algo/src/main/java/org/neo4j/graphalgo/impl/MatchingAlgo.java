package org.neo4j.graphalgo.impl;


import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.matchingAlgo.Matches;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatchingAlgo extends Algorithm<MatchingAlgo> {
    private final GraphDatabaseAPI dbService;
    private static final int PATH_END = -1;

    private Node capacityNode;
    private String capacityDistanceProperty;
    private Node originCityNode;
    private Node destCityNode;
    private String cityRouteRelation;
    private double maxDeadHeadPercentage;
    private double maxOffRoutePercentage;
    private String distancePropertyName;
    private String loadPickupRelName;
    private IntDoubleMap cocToLpcDistances;
    private LongLongMap loadLpcMap;
    private double totalCost;
    private final ProgressLogger progressLogger;
    private List<Matches> matches;

    public static final double NO_PATH_FOUND = -1.0;

    public MatchingAlgo(final Node capacityNode, final Node originCityNode, final Node destCityNode, final String cityRouteRelation, final ProcedureConfiguration configuration, final GraphDatabaseAPI dbService) {
        this.capacityNode = capacityNode;
        this.originCityNode = originCityNode;
        this.destCityNode = destCityNode;
        this.cityRouteRelation = cityRouteRelation;
        this.maxDeadHeadPercentage = configuration.get("maxDeadHeadPercentage", 87.5);
        this.maxOffRoutePercentage = configuration.get("maxOffRoutePercentage", 150.0);
        this.distancePropertyName =  configuration.get("distancePropertyName", "distance");
        this.capacityDistanceProperty =  configuration.get("capacityDistanceProperty", "total_distance");
        this.loadPickupRelName = configuration.get("loadPickupRelName", "LOAD_PICKUP_CITY");
        this.dbService = dbService;
        progressLogger = getProgressLogger();
    }

    public MatchingAlgo compute(){
        Iterable<Relationship> lpcsIterable = originCityNode.getRelationships(RelationshipType.withName(cityRouteRelation), Direction.BOTH);
        TraversalDescription routeTraversalDesc = dbService.traversalDescription()
                .relationships(RelationshipType.withName(cityRouteRelation), Direction.BOTH)
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .evaluator(Evaluators.atDepth(1))
                .order(BranchOrderingPolicies.PREORDER_BREADTH_FIRST);
        Traverser routesTraverser = routeTraversalDesc.traverse(originCityNode);
        TraversalDescription loadFromLpcTd = dbService.traversalDescription().breadthFirst()
                .relationships(RelationshipType.withName(loadPickupRelName), Direction.INCOMING)
                .evaluator(Evaluators.atDepth(1))
                .evaluator(Evaluators.includeIfAcceptedByAny());
        cocToLpcDistances = new IntDoubleScatterMap();
        loadLpcMap = new LongLongScatterMap();
        Map<Long, Node> loadMap = new HashMap<>();
        for ( Path route : routesTraverser )
        {
            int cocToLpcDistance = (int) route.lastRelationship().getProperty("distance");
            if( cocToLpcDistance < (int) capacityNode.getProperty(capacityDistanceProperty) * maxDeadHeadPercentage ){
                Node lpc = route.lastRelationship().getOtherNode(originCityNode);

            }
        }
        lpcsIterable.forEach(lpcRelation ->{
            int cocToLpcDistance = (int) lpcRelation.getProperty(distancePropertyName);
            if( cocToLpcDistance < (int) capacityNode.getProperty(capacityDistanceProperty) * maxDeadHeadPercentage ){
                Node lpc = lpcRelation.getOtherNode(originCityNode);
                Iterable<Relationship> loadRelationIterable = lpc.getRelationships(RelationshipType.withName(loadPickupRelName), Direction.INCOMING);
                loadRelationIterable.forEach(loadRelation -> {
                    Node load = loadRelation.getStartNode();
                    loadLpcMap.put(load.getId(), lpc.getId());
                    loadMap.put(load.getId(), load);
                });
                cocToLpcDistances.put((int) lpcRelation.getOtherNode(originCityNode).getId(), cocToLpcDistance);
            }
        });
        return this;
    }

    @Override
    public MatchingAlgo me() {
        return this;
    }

    @Override
    public MatchingAlgo release() {
        return this;
    }
}
