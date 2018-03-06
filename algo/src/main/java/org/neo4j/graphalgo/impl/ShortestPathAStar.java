package org.neo4j.graphalgo.impl;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;

public class ShortestPathAStar extends Algorithm<ShortestPathAStar> {
	
	private GraphDatabaseAPI dbService;
	private static final int PATH_END = -1;
	
	private Graph graph;
	private final int nodeCount;
    private IntDoubleMap gCosts;
    private IntDoubleMap fCosts;
    private double totalCost;
    private IntPriorityQueue openNodes;
    private IntIntMap path;
    private IntArrayDeque shortestPath;
    private SimpleBitSet closedNodes;
    private ProgressLogger progressLogger;
	
    public static final double NO_PATH_FOUND = -1.0;
    
    public ShortestPathAStar(Graph graph, GraphDatabaseAPI dbService) {
    		this.graph = graph;
    		this.dbService = dbService;
        nodeCount = Math.toIntExact(graph.nodeCount());
        gCosts = new IntDoubleScatterMap(nodeCount);
        fCosts = new IntDoubleScatterMap(nodeCount);
        openNodes = SharedIntPriorityQueue.min(
                nodeCount,
                fCosts,
                Double.MAX_VALUE);
        path = new IntIntScatterMap(nodeCount);
        closedNodes = new SimpleBitSet(nodeCount);
        shortestPath = new IntArrayDeque();
        progressLogger = getProgressLogger();
    }
    
    public ShortestPathAStar compute(long startNode, long goalNode) {
        return compute(startNode, goalNode, Direction.BOTH);
    }
    
    public ShortestPathAStar compute(long startNode, long goalNode, Direction direction) {
        reset();
        int _startNode = graph.toMappedNodeId(startNode);
        double _startlLat = getNodeCoordinate(_startNode, "latitude");
        double _startLon = getNodeCoordinate(_startNode, "longitude");
        int _goalNode = graph.toMappedNodeId(goalNode);
        double _goalLat = getNodeCoordinate(_goalNode, "latitude");
		double _goalLon = getNodeCoordinate(_goalNode, "longitude");
		double _heuristic = computeHeuristic(_startlLat, _startLon, _goalLat, _goalLon);
        gCosts.put(_startNode, 0.0);
        fCosts.put(_startNode, _heuristic);
        openNodes.add(_startNode, 0.0);
        run(_goalNode, _goalLat, _goalLon, direction);
        if (path.containsKey(_goalNode)) {
        		totalCost = gCosts.get(_goalNode);
        		int node = _goalNode;
        		while (node != PATH_END) {
                shortestPath.addFirst(node);
                node = path.getOrDefault(node, PATH_END);
            }
        }
        return this;
    }
    
    private void run(int goalNodeId, double goalLat, double goalLon, Direction direction) {
        while (!openNodes.isEmpty() && running()) {
            int currentNodeId = openNodes.pop();
            if (currentNodeId == goalNodeId) {
                return;
            }
            closedNodes.put(currentNodeId);
            double currentNodeCost = this.gCosts.getOrDefault(currentNodeId, Double.MAX_VALUE);
            graph.forEachRelationship(
            		currentNodeId,
            		direction,
            		(source, target, relationshipId, weight) -> {
            			double neighbourLat = getNodeCoordinate(target, "latitude");
            			double neighbourLon = getNodeCoordinate(target, "longitude");
            			double heuristic = computeHeuristic(neighbourLat, neighbourLon, goalLat, goalLon);
            			updateCosts(source, target, weight + currentNodeCost, heuristic);
            			if (!closedNodes.contains(target)) {
            				openNodes.add(target, 0);
            			}
            			return true;
            		});
            progressLogger.logProgress((double) currentNodeId / (nodeCount - 1));
        }
    }
    
    private double computeHeuristic(double lat1, double lon1, double lat2, double lon2) {
    		final int earthRadius = 6371;
    		final double kmToNM = 0.539957;
    		double latDistance = Math.toRadians(lat2 - lat1);
    		double lonDistance = Math.toRadians(lon2 - lon1);
    		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
    	            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
    	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
    	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    	    double distance = earthRadius * c * kmToNM;
    	    return distance;
    }
    
    private double getNodeCoordinate(int nodeId, String coordinateType) {
    		long neo4jId = graph.toOriginalNodeId(nodeId);
    		Node node = dbService.getNodeById(neo4jId);
    		return (double) node.getProperty(coordinateType);
    }
    
    private void updateCosts(int source, int target, double newCost, double heuristic) {
        double oldCost = gCosts.getOrDefault(target, Double.MAX_VALUE);
        if (newCost < oldCost) {
            gCosts.put(target, newCost);
            fCosts.put(target, newCost + heuristic);
            path.put(target, source);
        }
    }
    
    private void reset() {
        closedNodes.clear();
        openNodes.clear();
        gCosts.clear();
        path.clear();
        shortestPath.clear();
        totalCost = NO_PATH_FOUND;
    }
    
    public Stream<Result> resultStream() {
        return StreamSupport.stream(shortestPath.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.value), gCosts.get(cursor.value)));
    }

    public IntArrayDeque getFinalPath() {
        return shortestPath;
    }
    
    public double getTotalCost() {
        return totalCost;
    }

    public int getPathLength() {
        return shortestPath.size();
    }
    
    @Override
    public ShortestPathAStar me() {
        return this;
    }

    @Override
    public ShortestPathAStar release() {
        graph = null;
        gCosts = null;
        openNodes = null;
        path = null;
        shortestPath = null;
        closedNodes = null;
        return this;
    }
    
    public static class Result {

        /**
         * the neo4j node id
         */
        public final Long nodeId;
        /**
         * cost to reach the node from startNode
         */
        public final Double cost;

        public Result(Long nodeId, Double cost) {
            this.nodeId = nodeId;
            this.cost = cost;
        }
    }
}