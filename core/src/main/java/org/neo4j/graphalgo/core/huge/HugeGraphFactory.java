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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public HugeGraph build() {
        try {
            return importGraph();
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }


    private HugeGraph importGraph() throws EntityNotFoundException {
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker;
        HugeWeightMapping weights = hugeWeightMapping(tracker, dimensions.weightId(), setup.relationDefaultWeight);
        HugeIdMap mapping = loadHugeIdMap(tracker);
        HugeGraph graph = loadRelationships(dimensions, mapping, weights, concurrency, tracker, progress);
        progressLogger.logDone(tracker);
        return graph;
    }

    private HugeGraph loadRelationships(
            GraphDimensions dimensions,
            HugeIdMap mapping,
            HugeWeightMapping weights,
            int concurrency,
            AllocationTracker tracker,
            ImportProgress progress) {
        if (setup.loadAsUndirected) {
            return loadUndirectedRelationships(
                    dimensions,
                    mapping,
                    weights,
                    concurrency,
                    tracker,
                    progress);
        }

        final long nodeCount = dimensions.hugeNodeCount();
        final int[] relationId = dimensions.relationId();
        final int weightId = dimensions.weightId();

        HugeLongArray inOffsets = null;
        HugeLongArray outOffsets = null;
        ByteArray inAdjacency = null;
        ByteArray outAdjacency = null;
        if (setup.loadIncoming) {
            inOffsets = HugeLongArray.newArray(nodeCount, tracker);
            inAdjacency = ByteArray.newArray(0, tracker);
        }
        if (setup.loadOutgoing) {
            outOffsets = HugeLongArray.newArray(nodeCount, tracker);
            outAdjacency = ByteArray.newArray(nodeCount, tracker);
        }
        if (setup.loadIncoming || setup.loadOutgoing) {
            // needs final b/c of reference from lambda
            final HugeLongArray finalInOffsets = inOffsets;
            final HugeLongArray finalOutOffsets = outOffsets;
            final ByteArray finalInAdjacency = inAdjacency;
            final ByteArray finalOutAdjacency = outAdjacency;

            NodeQueue nodes = new NodeQueue(nodeCount);
            HugeRelationshipImporter[] tasks = new HugeRelationshipImporter[concurrency];
            Arrays.setAll(tasks, i -> new HugeRelationshipImporter(
                    api,
                    i,
                    nodes,
                    progress,
                    mapping,
                    finalInOffsets,
                    finalOutOffsets,
                    finalInAdjacency,
                    finalOutAdjacency,
                    false,
                    relationId,
                    weightId,
                    weights
            ));
            ParallelUtil.run(Arrays.asList(tasks), threadPool);
        }

        return new HugeGraphImpl(
                tracker,
                mapping,
                weights,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }

    private HugeGraph loadUndirectedRelationships(
            GraphDimensions dimensions,
            HugeIdMap mapping,
            HugeWeightMapping weights,
            int concurrency,
            AllocationTracker tracker,
            ImportProgress progress) {
        final long nodeCount = dimensions.hugeNodeCount();
        final int[] relationId = dimensions.relationId();
        final int weightId = dimensions.weightId();

        HugeLongArray offsets = HugeLongArray.newArray(nodeCount, tracker);
        ByteArray adjacency = ByteArray.newArray(0, tracker);

        NodeQueue nodes = new NodeQueue(nodeCount);
        HugeRelationshipImporter[] tasks = new HugeRelationshipImporter[concurrency];
        Arrays.setAll(tasks, i -> new HugeRelationshipImporter(
                api,
                i,
                nodes,
                progress,
                mapping,
                null,
                offsets,
                null,
                adjacency,
                true,
                relationId,
                weightId,
                weights
        ));
        ParallelUtil.run(Arrays.asList(tasks), threadPool);

        return new HugeGraphImpl(
                tracker,
                mapping,
                weights,
                null,
                adjacency,
                null,
                offsets
        );
    }

    private static final class NodeQueue {
        private final AtomicLong current = new AtomicLong();
        private final long max;

        NodeQueue(final long max) {
            this.max = max;
        }

        long next() {
            long nodeId = current.getAndIncrement();
            return nodeId < max ? nodeId : -1L;
        }
    }

    private static final class HugeRelationshipImporter extends StatementTask<Void, EntityNotFoundException> {
        private final int batchIndex;
        private final ImportProgress progress;
        private final NodeQueue nodes;
        private final HugeIdMap idMap;
        private final HugeLongArray inOffsets;
        private final HugeLongArray outOffsets;
        private final ByteArray.LocalAllocator inAllocator;
        private final ByteArray.LocalAllocator outAllocator;
        private final int[] relationId;
        private final int weightId;
        private final HugeWeightMapping weights;
        private final boolean undirected;

        HugeRelationshipImporter(
                GraphDatabaseAPI api,
                int batchIndex,
                NodeQueue nodes,
                ImportProgress progress,
                HugeIdMap idMap,
                HugeLongArray inOffsets,
                HugeLongArray outOffsets,
                ByteArray inAdjacency,
                ByteArray outAdjacency,
                boolean undirected,
                int[] relationId,
                int weightId,
                HugeWeightMapping weights) {
            super(api);
            this.batchIndex = batchIndex;
            this.progress = progress;
            this.nodes = nodes;
            this.idMap = idMap;
            this.inOffsets = inOffsets;
            this.outOffsets = outOffsets;
            this.inAllocator = inAdjacency != null ? inAdjacency.newAllocator() : null;
            this.outAllocator = outAdjacency != null ? outAdjacency.newAllocator() : null;
            this.relationId = relationId;
            this.weightId = weightId;
            this.weights = weights;
            this.undirected = undirected;
        }

        @Override
        public String threadName() {
            return "HugeRelationshipImport-" + batchIndex;
        }

        @Override
        public Void apply(final Statement statement) throws EntityNotFoundException {
            ReadOperations readOp = statement.readOperations();
            boolean shouldLoadWeights = weightId >= 0;
            HugeWeightMap weightMap = shouldLoadWeights ? (HugeWeightMap) weights : null;

            final RelationshipLoader loader;
            if (undirected) {
                assert outOffsets != null;
                assert outAllocator != null;

                outAllocator.prepare();
                final VisitRelationship visitIn;
                final VisitRelationship visitOut;
                if (shouldLoadWeights) {
                    visitIn = new VisitIncomingNoWeight(idMap);
                    visitOut = new VisitUndirectedOutgoingWithWeight(readOp, idMap, weightMap, weightId);
                } else {
                    visitIn = new VisitIncomingNoWeight(idMap);
                    visitOut = new VisitOutgoingNoWeight(idMap);
                }
                loader = new ReadUndirected(readOp, outOffsets, outAllocator, relationId, visitOut, visitIn);
            } else {
                RelationshipLoader load = null;
                if (outAllocator != null) {
                    outAllocator.prepare();
                    final VisitRelationship visitOut;
                    if (shouldLoadWeights) {
                        visitOut = new VisitOutgoingWithWeight(readOp, idMap, weightMap, weightId);
                    } else {
                        visitOut = new VisitOutgoingNoWeight(idMap);
                    }
                    load = new ReadOutgoing(readOp, outOffsets, outAllocator, relationId, visitOut);
                }
                if (inAllocator != null) {
                    inAllocator.prepare();
                    final VisitRelationship visitIn;
                    if (shouldLoadWeights) {
                        visitIn = new VisitIncomingWithWeight(readOp, idMap, weightMap, weightId);
                    } else {
                        visitIn = new VisitIncomingNoWeight(idMap);
                    }
                    if (load != null) {
                        load = new ReadBoth((ReadOutgoing) load, visitIn, inOffsets, inAllocator);
                    } else {
                        load = new ReadIncoming(readOp, inOffsets, inAllocator, relationId, visitIn);
                    }
                }
                if (load != null) {
                    loader = load;
                } else {
                    loader = new ReadNothing(readOp, relationId);
                }
            }

            NodeQueue nodes = this.nodes;
            long nodeId;
            while ((nodeId = nodes.next()) != -1L) {
                loader.load(idMap.toOriginalNodeId(nodeId), nodeId);
                progress.relProgress();
            }
            return null;
        }
    }
}
