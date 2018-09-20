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
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * specialized dijkstra impl. for YensKShortestPath
 *
 * @author mknblch
 */
public class Dijkstra {

    // initial weighted path capacity

    public static final int INITIAL_CAPACITY = 64;

    private static final int PATH_END = -1;
    private final Graph graph;

    private final int nodeCount;

    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;
    // node to cost map
    private final IntDoubleMap costs;
    // next node priority queue
    private final PriorityQueue<Element> queue;
    // auxiliary path map
    private final IntIntMap path;
    // visited set
    private final BitSet visited;
    // visited filter
    private RelationshipConsumer filter = (sourceNodeId, targetNodeId, relationId) -> true;
    // traverse direction
    private Direction direction = Direction.BOTH;
    // iteration depth
    private int[] depth;


    public Dijkstra(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        costs = new IntDoubleScatterMap(nodeCount);
        queue = new PriorityQueue<>();
        path = new IntIntScatterMap(nodeCount);
        visited = new BitSet(nodeCount);
        depth = new int[nodeCount];
    }

    /**
     * set termination flag
     * @param terminationFlag the flag
     * @return this
     */
    public Dijkstra withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    /**
     * unset filter
     * @return this
     */
    public Dijkstra withoutFilter() {
        this.filter = (sourceNodeId, targetNodeId, relationId) -> true;
        return this;
    }

    /**
     * set relationship / node - filter
     * @param filter filter
     * @return this
     */
    public Dijkstra withFilter(RelationshipConsumer filter) {
        this.filter = filter;
        return this;
    }

    /**#
     * set traverse direction
     * @param direction the direction
     * @return this
     */
    public Dijkstra withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    /**
     * compute shortest path from sourceNode to targetNode
     * @param sourceNode mapped source node id
     * @param targetNode mapped target node id
     * @return an optional path
     */
    public Optional<WeightedPath> compute(int sourceNode, int targetNode) {
        return compute(sourceNode, targetNode, Integer.MAX_VALUE);
    }

    /**
     * compute shortest path from sourceNode to targetNode
     * @param sourceNode mapped source node id
     * @param targetNode mapped target node id
     * @param maxDepth maximum traversal depth
     * @return an optional path
     */
    public Optional<WeightedPath> compute(int sourceNode, int targetNode, int maxDepth) {
        if (!dijkstra(sourceNode, targetNode, direction, maxDepth)) {
            return Optional.empty();
        }
        int last = targetNode;
        final WeightedPath resultPath = new WeightedPath(INITIAL_CAPACITY);
        while (last != PATH_END) {
            resultPath.append(last);
            last = path.getOrDefault(last, PATH_END);
        }
        return Optional.of(resultPath
                .withWeight(costs.get(targetNode))
                .reverse());
    }

    /**
     * calc path
     * @return true if a path has been found, false otherwise
     */
    private boolean dijkstra(int source, int target, Direction direction, int maxDepth) {
        costs.clear();
        queue.clear();
        path.clear();
        visited.clear();
        costs.put(source, 0.0);
        queue.add(new Element(source, 0.0));
        Arrays.fill(depth, 0);
        depth[source] = 1;
        while (queue.size() > 0 && terminationFlag.running()) {
            final Element k = queue.poll();
            final int node = k.key;
            final int d = depth[node];
            if (d >= maxDepth) {
                continue;
            }
            if (node == target) {
                return true;
            }
            visited.set(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    direction, (s, t, relId) -> {
                        if (!filter.accept(s, t, relId)) {
                            return true;
                        }
                        final double w = graph.weightOf(s, t);
                        if (!visited.get(t)) {
                            depth[t] = depth[node] + 1;
                            queue.add(new Element(t, w));
                            updateCosts(s, t, costs + w);
                        }
                        return terminationFlag.running();
                    });
        }
        return false;
    }


    /**
     * update cost map
     */
    private void updateCosts(int source, int target, double newCosts) {
        double oldCosts = costs.getOrDefault(target, Double.MAX_VALUE);
        if (newCosts < oldCosts) {
            costs.put(target, newCosts);
            path.put(target, source);
        }
    }

    class Element implements Comparable<Element> {

        public final Integer key;
        public final double value;

        Element(Integer key, double value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Element o) {
            return Double.compare(value, o.value);
        }
    }

}
