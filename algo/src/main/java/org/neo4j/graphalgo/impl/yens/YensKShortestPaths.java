package org.neo4j.graphalgo.impl.yens;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.*;

/**
 * Yens Algorithm for computing K shortest Paths
 *
 * @author mknobloch
 */
public class YensKShortestPaths extends Algorithm<YensKShortestPaths> {

    private Dijkstra dijkstra;

    private HeavyGraph graph;
    private List<WeightedPath> paths;
    private PriorityQueue<WeightedPath> priorityQueue;
    private ProgressLogger progressLogger;

    public YensKShortestPaths(HeavyGraph graph) {
        this.graph = graph;
        progressLogger = getProgressLogger();
        dijkstra = new Dijkstra(graph);
        paths = new ArrayList<>();
        priorityQueue = new PriorityQueue<>(Comparator.comparingDouble(o -> WeightedPath.getCosts(graph, o)));
    }

    public List<WeightedPath> getPaths() {
        return paths;
    }

    /**
     * compute shortest path between startNode and goalNode
     *
     * @return itself
     */
    public YensKShortestPaths compute(long startNode, long goalNode, int k) {
        return compute(startNode, goalNode, Direction.BOTH, k);
    }

    public YensKShortestPaths compute(long startNode, long goalNode, Direction direction, int k) {
        yens(startNode, goalNode, direction, k);
        return this;
    }

    private void yens(long startNode, long goalNode, Direction direction, int maxPaths) {
        final IntScatterSet nodeBlackList = new IntScatterSet();
        final LongScatterSet edgeBlackList = new LongScatterSet();
        final int goal = graph.toMappedNodeId(goalNode);

        dijkstra.withDirection(direction)
                .withFilter((s, t, r) ->
                        !nodeBlackList.contains(t) &&
                        !edgeBlackList.contains(RawValues.combineIntInt(s, t)))
                .compute(graph.toMappedNodeId(startNode), goal)
                .ifPresent(paths::add);

        for (int k = 1; k < maxPaths && paths.size() >= k; k++) {
            System.out.println("k = " + k);
            final WeightedPath basePath = paths.get(k - 1);

            if (basePath == null) {
                return;
            }

            System.out.println("basePath = " + basePath);

            for (int i = 0; i < basePath.size() - 1; i++) {
                final int spurNode = basePath.node(i);

                System.out.println("spurNode = " + spurNode);

                final WeightedPath rootPath = basePath.pathTo(i);
                System.out.println("rootPath = " + rootPath);


                for (Iterator<WeightedPath> iterator = paths.iterator(); iterator.hasNext(); ) {
                    final WeightedPath p = iterator.next();
                    System.out.println(" comparing to p = " + p + " to index " + i);
                    if (p.equalsTo(rootPath, i)) {
                        final long edge = p.edge(i);
                        System.out.println(" disabling Edge " + RawValues.getHead(edge) + "-" + RawValues.getTail(edge));
                        edgeBlackList.add(edge);
                    }
                }
                rootPath.forEachDo(rootPathNode -> {
                    if (rootPathNode != spurNode) {
                        nodeBlackList.add(rootPathNode);
                    }
                });
                final Optional<WeightedPath> spurPathOpt = dijkstra.compute(spurNode, goal);
                if (!spurPathOpt.isPresent()) {
                    System.out.println(" inner dijkstra did not found a new path");
                    break;
                }

                final WeightedPath spurPath = spurPathOpt.get();
                System.out.println("spurPath = " + spurPath);

                final WeightedPath newPath = WeightedPath.concat(rootPath, spurPath);

                System.out.println("newPath: " + newPath.toString());

                priorityQueue.add(newPath);

                nodeBlackList.clear();
                edgeBlackList.clear();
            }
            if (priorityQueue.isEmpty()) {
                return;
            }
            paths.add(priorityQueue.remove());
        }
    }

    @Override
    public YensKShortestPaths me() {
        return this;
    }

    @Override
    public YensKShortestPaths release() {
        graph = null;
        dijkstra = null;
        paths = null;
        priorityQueue = null;
        return this;
    }

    /**
     * Result DTO
     */
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
