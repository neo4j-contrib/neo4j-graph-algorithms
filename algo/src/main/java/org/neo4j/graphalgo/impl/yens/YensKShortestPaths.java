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
 * Yen's k-shortest-paths Algorithm
 *
 * @author mknblch
 */
public class YensKShortestPaths extends Algorithm<YensKShortestPaths> {

    private Dijkstra dijkstra;
    private Graph graph;
    private List<WeightedPath> shortestPaths;
    private PriorityQueue<WeightedPath> heap;
    private ProgressLogger progressLogger;

    public YensKShortestPaths(Graph graph) {
        this.graph = graph;
        progressLogger = getProgressLogger();
        dijkstra = new Dijkstra(graph);
        shortestPaths = new ArrayList<>();
        // TODO evaluate costs once!
        heap = new PriorityQueue<>(Comparator.comparingDouble(o -> WeightedPath.getCosts(graph, o)));
    }

    /**
     * retrieve the list of shortest paths
     * @return
     */
    public List<WeightedPath> getPaths() {
        return shortestPaths;
    }

    /**
     * compute at most k shortest paths between startNode and goalNode
     * @return itself
     */
    public YensKShortestPaths compute(long startNode, long goalNode, int k) {
        return compute(startNode, goalNode, Direction.OUTGOING, k);
    }

    /**
     * compute at most k shortest paths between startNode and goalNode
     * while using only supplied traversal direction
     * @return itself
     */
    public YensKShortestPaths compute(long startNode, long goalNode, Direction direction, int k) {
        yens(k,
                graph.toMappedNodeId(startNode),
                graph.toMappedNodeId(goalNode),
                direction);
        return this;
    }

    private void yens(int k, int start, int goal, Direction direction) {
        final IntScatterSet nodeBlackList = new IntScatterSet();
        final LongScatterSet edgeBlackList = new LongScatterSet();
        // clear result of previous execution
        shortestPaths.clear();
        // equip dijkstra with a node and edge filter and set its traversal direction
        dijkstra.withDirection(direction)
                .withFilter((s, t, r) ->
                        !nodeBlackList.contains(t) &&
                        !edgeBlackList.contains(RawValues.combineIntInt(s, t)))
                .compute(start, goal) // compute the best shortest path first
                .ifPresent(shortestPaths::add);

        if (shortestPaths.isEmpty()) {
            // not a single path found
            return;
        }

        // keep running until k elements have been found or no further shortest path is possible
        for (int n = 1; n < k; n++) {
            // retrieve head of the queue
            final WeightedPath basePath = shortestPaths.get(shortestPaths.size() - 1);
            System.out.println("basePath = " + basePath);
            // The spur node ranges from the first node to the next to last node in the previous k-shortest path.
            for (int i = basePath.size() - 2; i >= 0; i--) {
                // we don't alter the graph therefore we clear the filters at the beginning of each iteration.
                nodeBlackList.clear();
                edgeBlackList.clear();
                // Spur node is retrieved from the previous k-shortest path.
                final int spurNode = basePath.node(i);
                // The sequence of nodes from the source to the spur node of the previous k-shortest path.
                final WeightedPath rootPath = basePath.pathTo(i);
                // check each of th>e known shortest paths
                System.out.println("shortestPaths = " + shortestPaths);
                for (Iterator<WeightedPath> iterator = shortestPaths.iterator(); iterator.hasNext(); ) {
                    final WeightedPath p = iterator.next();
                    if (rootPath.equalsTo(p, i + 1)) {
                        // blacklist the rels that are part of the previous shortest paths with the same root path.
                        final long edge = p.edge(i);
                        if (edgeBlackList.add(edge)) {

                            System.out.println("blacklist: " + RawValues.getHead(p.edge(i)) + " -> " + RawValues.getTail(p.edge(i)));
                        }
                    }
                }
                // blacklist nodes in rootPath if not spurNode
                rootPath.forEachDo(rootPathNode -> {
                    if (rootPathNode != spurNode) {
                        nodeBlackList.add(rootPathNode);
                    }
                });
                // Calculate the spur path from the spur node to the sink.
                final Optional<WeightedPath> spurPathOpt = dijkstra.compute(spurNode, goal);
                // no path found, continue
                if (!spurPathOpt.isPresent()) {
                    continue;
                }
                // new candidate is the concatenation of rootPath and the spurPath.
                rootPath.dropTail();
                final WeightedPath concat = WeightedPath.concat(rootPath, spurPathOpt.get());
                System.out.println(rootPath + " + " + spurPathOpt.get() + " = " + concat);
                if (!heap.contains(concat)) {

                    heap.add(concat);
                }
            }
            // no new candidates have been found.
            if (heap.isEmpty()) {
                break;
            }
            // add the best candidate (with lowest weight) to the result set.
            System.out.println("heap = " + heap);
            System.out.println("adding " + heap.peek());
            shortestPaths.add(heap.remove());
            System.out.println();
            heap.clear();
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
        shortestPaths = null;
        heap = null;
        return this;
    }

    /**
     * concatenates head and tail. due to the implementation we have
     * a duplicate node at the end of head and start of tail. this
     * is considered by the logic.
     * @return concatenation of head and tail
     */
    private static WeightedPath concat(WeightedPath head, WeightedPath tail) {
        final int off = head.size() + tail.size() - 1;
        final int[] data = new int[off];
        System.arraycopy(head.nodes, 0, data, 0, head.size() - 1);
        System.arraycopy(tail.nodes, 0, data, head.size() - 1, tail.size());
        final WeightedPath weightedPath = new WeightedPath(data, off);
        return weightedPath;
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
