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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeRelationshipIntersect;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * @author mknblch
 */
public class TriangleCountQueue extends TriangleCountBase<double[], TriangleCountQueue> {

    private ExecutorService executorService;
    private final int concurrency;
    private final Direction direction;
    private final AtomicInteger queue;
    private final LongAdder triangleCount;
    private double averageClusteringCoefficient;

    public TriangleCountQueue(Graph graph, ExecutorService executorService, int concurrency) {
        super(graph);
        this.executorService = executorService;
        this.concurrency = concurrency;
        triangleCount = new LongAdder();
        direction = graph instanceof HugeGraph ? Direction.OUTGOING : Direction.BOTH;
        queue = new AtomicInteger();
    }

    @Override
    public long getTriangleCount() {
        return triangleCount.longValue();
    }

    @Override
    double coefficient(final int node) {
        return calculateCoefficient(node, direction);
    }

    @Override
    public double[] getClusteringCoefficients() {
        final double[] coefficient = new double[nodeCount];
        double sum = 0;
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(i, direction);
            coefficient[i] = c;
            sum += c;
        }
        averageClusteringCoefficient = sum / nodeCount;
        return coefficient;
    }

    @Override
    public final double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient;
    }

    @Override
    public TriangleCountQueue release() {
        super.release();
        executorService = null;
        return this;
    }

    @Override
    void runCompute() {
        queue.set(0);
        triangleCount.reset();
        averageClusteringCoefficient = 0.0;

        // create tasks
        final Collection<? extends Runnable> tasks;
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            tasks = ParallelUtil.tasks(concurrency, () -> new HugeTask(hugeGraph));
        } else {
            tasks = ParallelUtil.tasks(concurrency, Task::new);
        }

        // run
        ParallelUtil.run(tasks, executorService);
    }

    @Override
    void onTriangle() {
        triangleCount.increment();
    }

    private class Task implements Runnable {

        @Override
        public void run() {
            final IntStack nodes = new IntStack();
            final TerminationFlag flag = getTerminationFlag();
            final int[] head = new int[1];
            while ((head[0] = queue.getAndIncrement()) < nodeCount) {
                graph.forEachRelationship(head[0], D, (s, t, r) -> {
                    if (t > s) {
                        nodes.push(t);
                    }
                    return flag.running();
                });
                while (!nodes.isEmpty()) {
                    final int node = nodes.pop();
                    graph.forEachRelationship(node, D, (s, t, r) -> {
                        if (t > s && graph.exists(t, head[0], D)) {
                            exportTriangle(head[0], s, t);
                        }
                        return flag.running();
                    });
                }
                nodeVisited();
            }
        }
    }

    private class HugeTask implements Runnable, HugeRelationshipConsumer {

        private HugeRelationshipIntersect hg;

        private int degree;
        private long[] intersect;

        HugeTask(HugeGraph graph) {
            hg = graph.intersectionCopy();
            intersect = new long[0];
        }

        @Override
        public void run() {
            int node;
            while ((node = queue.getAndIncrement()) < nodeCount && running()) {
                degree = hg.degree(node);
                hg.forEachRelationship(node, this);
                nodeVisited();
            }
        }

        @Override
        public boolean accept(long nodeA, long nodeB) {
            if (nodeB > nodeA) {
                final int required = Math.min(degree, hg.degree(nodeB));
                long[] ts = grow(required);
                final int len = hg.intersect(nodeA, nodeB, ts, 0);
                for (int i = 0; i < len; i++) {
                    exportTriangle((int) nodeA, (int) nodeB, (int) ts[i]);
                }
            }
            // TODO: benchmark against return true
            return running();
        }

        private long[] grow(int minSize) {
            return intersect.length >= minSize ? intersect : (intersect = new long[ArrayUtil.oversize(minSize, Long.BYTES)]);
        }
    }
}
