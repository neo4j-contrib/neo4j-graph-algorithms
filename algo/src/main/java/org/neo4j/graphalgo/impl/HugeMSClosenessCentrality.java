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

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.AtomicIntArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.HugeBfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.HugeMultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.function.LongToIntFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * @author mknblch
 */
public class HugeMSClosenessCentrality extends MSBFSCCAlgorithm<HugeMSClosenessCentrality> {

    private HugeGraph graph;
    private AtomicIntArray farness;

    private final int concurrency;
    private final ExecutorService executorService;
    private final long nodeCount;
    private final AllocationTracker tracker;

    public HugeMSClosenessCentrality(
            HugeGraph graph,
            AllocationTracker tracker,
            int concurrency,
            ExecutorService executorService) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.tracker = tracker;
        farness = AtomicIntArray.newArray(nodeCount, this.tracker);
    }

    @Override
    public HugeMSClosenessCentrality compute() {

        final ProgressLogger progressLogger = getProgressLogger();

        final HugeBfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * depth);
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new HugeMultiSourceBFS(
                graph,
                graph,
                Direction.OUTGOING,
                consumer,
                tracker)
                .run(concurrency, executorService);

        return this;
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        final double k = nodeCount - 1;
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<AtomicIntArray>)
                        (data, nodeId) -> centrality(data.get(nodeId), k));
    }

    @Override
    public LongToIntFunction farness() {
        return farness::get;
    }

    @Override
    public Stream<MSClosenessCentrality.Result> resultStream() {
        final double k = nodeCount - 1;
        return LongStream.range(0L, nodeCount)
                .mapToObj(nodeId -> new MSClosenessCentrality.Result(
                        graph.toOriginalNodeId(nodeId),
                        centrality(farness.get(nodeId), k)));
    }

    @Override
    public HugeMSClosenessCentrality me() {
        return this;
    }

    @Override
    public HugeMSClosenessCentrality release() {
        graph = null;
        farness = null;
        return this;
    }
}
