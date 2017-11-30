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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * @author mknblch
 */
public class TriangleStream extends Algorithm<TriangleStream> {

    public static final Direction D = Direction.BOTH;
    private Graph graph;
    private ExecutorService executorService;
    private final AtomicInteger queue;
    private final int concurrency;
    private final int nodeCount;
    private AtomicInteger visitedNodes;
    private AtomicInteger runningThreads;
    private BlockingQueue<Result> resultQueue;

    public TriangleStream(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.resultQueue = new LinkedBlockingQueue<>();
        runningThreads = new AtomicInteger();
        visitedNodes = new AtomicInteger();
        queue = new AtomicInteger();
    }

    @Override
    public TriangleStream me() {
        return this;
    }

    @Override
    public TriangleStream release() {
        visitedNodes = null;
        runningThreads = null;
        resultQueue = null;
        graph = null;
        executorService = null;
        return this;
    }

    public Stream<Result> resultStream() {
        submitTasks();
        final TerminationFlag flag = getTerminationFlag();
        final Iterator<Result> it = new Iterator<Result>() {

            @Override
            public boolean hasNext() {
                return flag.running() && (runningThreads.get() > 0 || !resultQueue.isEmpty());
            }

            @Override
            public Result next() {
                Result result = null;
                try {
                    while (hasNext() && result == null) {
                        result = resultQueue.poll(1, TimeUnit.SECONDS);
                    }
                    return result;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(it, 0), false)
                .filter(Objects::nonNull);
    }

    private void submitTasks() {
        queue.set(0);
        runningThreads.set(0);
        final ArrayList<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, getTerminationFlag(), executorService);
    }

    private class Task implements Runnable {

        private final Graph graph;

        private Task() {
            this.graph = TriangleStream.this.graph;
        }

        @Override
        public void run() {
            final IntStack nodes = new IntStack();
            final TerminationFlag flag = getTerminationFlag();
            final ProgressLogger progressLogger = getProgressLogger();
            final int[] k = {0};
            while ((k[0] = queue.getAndIncrement()) < nodeCount) {
                graph.forEachRelationship(k[0], D, (s, t, r) -> {
                    if (t > s) {
                        nodes.add(t);
                    }
                    return flag.running();
                });
                while (!nodes.isEmpty()) {
                    final int node = nodes.pop();
                    graph.forEachRelationship(node, D, (s, t, r) -> {
                        if (t > s && graph.exists(t, k[0], Direction.BOTH)) {
                            try {
                                resultQueue.put(new Result(
                                        graph.toOriginalNodeId(k[0]),
                                        graph.toOriginalNodeId(s),
                                        graph.toOriginalNodeId(t)));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return flag.running();
                    });
                }
                progressLogger.logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
        }
    }

    public static class Result {

        public final long nodeA;
        public final long nodeB;
        public final long nodeC;

        public Result(long nodeA, long nodeB, long nodeC) {
            this.nodeA = nodeA;
            this.nodeB = nodeB;
            this.nodeC = nodeC;
        }

        @Override
        public String toString() {
            return "Triangle{" +
                    nodeA +
                    ", " + nodeB +
                    ", " + nodeC +
                    '}';
        }
    }
}
