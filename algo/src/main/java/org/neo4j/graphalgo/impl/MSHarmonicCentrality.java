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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Harmonic Centrality Algorithm
 *
 * @author mknblch
 */
public class MSHarmonicCentrality extends Algorithm<MSHarmonicCentrality> {

    private Graph graph;
    private final AtomicDoubleArray inverseFarness;
    private final int concurrency;
    private ExecutorService executorService;
    private final int nodeCount;

    public MSHarmonicCentrality(Graph graph, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        inverseFarness = new AtomicDoubleArray(nodeCount);
    }

    public MSHarmonicCentrality compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            inverseFarness.add(nodeId, sourceNodeIds.size() * (1.0 / depth));
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };
        new MultiSourceBFS(graph, graph, Direction.BOTH, consumer)
                .run(concurrency, executorService);
        return this;
    }

    public Stream<Result> resultStream() {
        final double k = 1.0 / (nodeCount - 1);
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        inverseFarness.get(nodeId) *  k));
    }

    public void export(final String propertyName, final Exporter exporter) {
        final double k = 1.0 / (nodeCount - 1);
        exporter.write(
                propertyName,
                inverseFarness,
                (PropertyTranslator.OfDouble<AtomicDoubleArray>)
                        (data, nodeId) -> data.get((int) nodeId) * k);
    }

    @Override
    public MSHarmonicCentrality me() {
        return this;
    }

    @Override
    public MSHarmonicCentrality release() {
        graph = null;
        executorService = null;
        return this;
    }

    public final double[] exportToArray() {
        return resultStream()
                .limit(Integer.MAX_VALUE)
                .mapToDouble(r -> r.centrality)
                .toArray();
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", centrality=" + centrality +
                    '}';
        }
    }
}
