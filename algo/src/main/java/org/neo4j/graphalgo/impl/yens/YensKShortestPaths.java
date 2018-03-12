/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.yens;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.*;

/**
 * Yens Algorithm for computing k-shortest-paths
 *
 * @author mknobloch
 */
public class YensKShortestPaths extends Algorithm<YensKShortestPaths> {

    private Dijkstra dijkstra;

    private Graph graph;
    private List<WeightedPath> A;
    private PriorityQueue<WeightedPath> B;
    private ProgressLogger progressLogger;

    public YensKShortestPaths(Graph graph) {
        this.graph = graph;
        progressLogger = getProgressLogger();
        dijkstra = new Dijkstra(graph);
        A = new ArrayList<>();
        B = new PriorityQueue<>(Comparator.comparingDouble(o -> -WeightedPath.getCosts(graph, o)));
    }

    public List<WeightedPath> getPaths() {
        return A;
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

    private void yens(long startNode, long goalNode, Direction direction, int K) {
        final IntScatterSet nodeBlackList = new IntScatterSet();
        final LongScatterSet edgeBlackList = new LongScatterSet();
        final int goal = graph.toMappedNodeId(goalNode);

        dijkstra.withDirection(direction)
                .withFilter((s, t, r) ->
                        !nodeBlackList.contains(t) &&
                        !edgeBlackList.contains(RawValues.combineIntInt(s, t)))
                .compute(graph.toMappedNodeId(startNode), goal)
                .ifPresent(A::add);

        for (int k = 1; k < K; k++) {
            final WeightedPath basePath = A.get(k-1);
            System.out.println("k = " + k);
            System.out.println("basePath = " + basePath);

//            for (int i = basePath.size()-1; i >= 0; i--) {
            for (int i = 0; i < basePath.size() - 1; i++) {
                final int spurNode = basePath.node(i);
                System.out.println("i = " + i);
                final WeightedPath rootPath = basePath.pathTo(i);
                System.out.println("  rootPath = " + rootPath);
                for (Iterator<WeightedPath> iterator = A.iterator(); iterator.hasNext(); ) {
                    final WeightedPath p = iterator.next();
                    if (rootPath.equalsTo(p, i)) {
//                    if (p.equalsTo(rootPath, i)) {

                        System.out.println("'" + p.pathTo(i) + "' is equal to '" + rootPath.pathTo(i) + "'");

                        final long edge = p.edge(i);

                        System.out.println("Blacklisting " +
                                RawValues.getHead(edge) + " -> " +
                                RawValues.getTail(edge)
                        );

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
                    break;
                }
                final WeightedPath spurPath = spurPathOpt.get();
                final WeightedPath newPath = WeightedPath.concat(rootPath, spurPath);
                B.add(newPath);
                nodeBlackList.clear();
                edgeBlackList.clear();
            }
            if (B.isEmpty()) {
                break;
            }
            final WeightedPath remove = B.remove();
            System.out.println("remove = " + remove);
            A.add(remove);
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
        A = null;
        B = null;
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
