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
import org.neo4j.graphalgo.core.utils.ApproximatedImportProgress;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public HugeGraph build() {
        return importGraph();
    }

    @Override
    protected ImportProgress importProgress(
            final ProgressLogger progressLogger,
            final GraphDimensions dimensions,
            final GraphSetup setup) {

        // ops for scanning degrees
        long relOperations = 0L;

        // batching for undirected double the amount of rels imported
        if (setup.loadIncoming || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }
        if (setup.loadOutgoing || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }

        return new ApproximatedImportProgress(
                progressLogger,
                setup.tracker,
                dimensions.hugeNodeCount(),
                relOperations
        );
    }

    private HugeGraph importGraph() {
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker;
        HugeWeightMapping weights = hugeWeightMapping(tracker, dimensions.relWeightId(), setup.relationDefaultWeight);
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
        final int[] relationId = dimensions.relationshipTypeId();
        final int weightId = dimensions.relWeightId();

        HugeLongArray inOffsets = null;
        HugeLongArray outOffsets = null;
        HugeAdjacencyBuilder inAdjacency = null;
        HugeAdjacencyBuilder outAdjacency = null;
        if (setup.loadIncoming) {
            inOffsets = HugeLongArray.newArray(nodeCount, tracker);
            inAdjacency = new HugeAdjacencyBuilder(tracker);
        }
        if (setup.loadOutgoing) {
            outOffsets = HugeLongArray.newArray(nodeCount, tracker);
            outAdjacency = new HugeAdjacencyBuilder(tracker);
        }
        if (setup.loadIncoming || setup.loadOutgoing) {
            // needs final b/c of reference from lambda
            final HugeLongArray finalInOffsets = inOffsets;
            final HugeLongArray finalOutOffsets = outOffsets;
            final HugeAdjacencyBuilder finalInAdjacency = inAdjacency;
            final HugeAdjacencyBuilder finalOutAdjacency = outAdjacency;

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

        return HugeAdjacencyBuilder.apply(
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
        final int[] relationId = dimensions.relationshipTypeId();
        final int weightId = dimensions.relWeightId();

        HugeLongArray offsets = HugeLongArray.newArray(nodeCount, tracker);
        HugeAdjacencyBuilder adjacency = new HugeAdjacencyBuilder(tracker);

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

        return HugeAdjacencyBuilder.apply(
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

    private static final class HugeRelationshipImporter extends StatementAction {
        private final int batchIndex;
        private final ImportProgress progress;
        private final NodeQueue nodes;
        private final HugeIdMap idMap;
        private final HugeLongArray inOffsets;
        private final HugeLongArray outOffsets;
        private final HugeAdjacencyBuilder inAllocator;
        private final HugeAdjacencyBuilder outAllocator;
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
                HugeAdjacencyBuilder inAdjacency,
                HugeAdjacencyBuilder outAdjacency,
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
            this.inAllocator = inAdjacency != null ? inAdjacency.threadLocalCopy() : null;
            this.outAllocator = outAdjacency != null ? outAdjacency.threadLocalCopy() : null;
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
        public void accept(final KernelTransaction transaction) {
            Read readOp = transaction.dataRead();
            CursorFactory cursors = transaction.cursors();
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
                    visitOut = new VisitUndirectedOutgoingWithWeight(readOp, cursors, idMap, weightMap, weightId);
                } else {
                    visitIn = new VisitIncomingNoWeight(idMap);
                    visitOut = new VisitOutgoingNoWeight(idMap);
                }
                loader = new ReadUndirected(transaction, outOffsets, outAllocator, relationId, visitOut, visitIn);
            } else {
                RelationshipLoader load = null;
                if (outAllocator != null) {
                    outAllocator.prepare();
                    final VisitRelationship visitOut;
                    if (shouldLoadWeights) {
                        visitOut = new VisitOutgoingWithWeight(readOp, cursors, idMap, weightMap, weightId);
                    } else {
                        visitOut = new VisitOutgoingNoWeight(idMap);
                    }
                    load = new ReadOutgoing(transaction, outOffsets, outAllocator, relationId, visitOut);
                }
                if (inAllocator != null) {
                    inAllocator.prepare();
                    final VisitRelationship visitIn;
                    if (shouldLoadWeights) {
                        visitIn = new VisitIncomingWithWeight(readOp, cursors, idMap, weightMap, weightId);
                    } else {
                        visitIn = new VisitIncomingNoWeight(idMap);
                    }
                    if (load != null) {
                        load = new ReadBoth((ReadOutgoing) load, visitIn, inOffsets, inAllocator);
                    } else {
                        load = new ReadIncoming(transaction, inOffsets, inAllocator, relationId, visitIn);
                    }
                }
                if (load != null) {
                    loader = load;
                } else {
                    loader = new ReadNothing(transaction, relationId);
                }
            }

            try (NodeCursor nodeCursor = cursors.allocateNodeCursor()) {
                NodeQueue nodes = this.nodes;
                int imported;
                long sourceNodeId, nodeId;
                while ((nodeId = nodes.next()) != -1L) {
                    sourceNodeId = idMap.toOriginalNodeId(nodeId);
                    readOp.singleNode(sourceNodeId, nodeCursor);
                    if (nodeCursor.next()) {
                        imported = loader.load(nodeCursor, nodeId);
                        progress.relationshipsImported(imported);
                    }
                }
            }
        }
    }
}
