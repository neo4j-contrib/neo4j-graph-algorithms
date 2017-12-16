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

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.DeltaEncoding;
import org.neo4j.graphalgo.core.utils.paged.LongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
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

        LongArray inOffsets = null;
        LongArray outOffsets = null;
        ByteArray inAdjacency = null;
        ByteArray outAdjacency = null;
        if (setup.loadIncoming) {
            inOffsets = LongArray.newArray(nodeCount, tracker);
            inAdjacency = ByteArray.newArray(0, tracker);
        }
        if (setup.loadOutgoing) {
            outOffsets = LongArray.newArray(nodeCount, tracker);
            outAdjacency = ByteArray.newArray(nodeCount, tracker);
        }
        if (setup.loadIncoming || setup.loadOutgoing) {
            // needs final b/c of reference from lambda
            final LongArray finalInOffsets = inOffsets;
            final LongArray finalOutOffsets = outOffsets;
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

        LongArray offsets = LongArray.newArray(nodeCount, tracker);
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

    @FunctionalInterface
    private interface RelationshipLoader {
        void apply(long neoId, long nodeId) throws EntityNotFoundException;
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
        private final LongArray inOffsets;
        private final LongArray outOffsets;
        private final ByteArray.LocalAllocator inAllocator;
        private final ByteArray.LocalAllocator outAllocator;
        private final int[] relationId;
        private final int weightId;
        private final HugeWeightMapping weights;
        private final boolean loadsBoth;
        private final boolean undirected;

        HugeRelationshipImporter(
                GraphDatabaseAPI api,
                int batchIndex,
                NodeQueue nodes,
                ImportProgress progress,
                HugeIdMap idMap,
                LongArray inOffsets,
                LongArray outOffsets,
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
            this.loadsBoth = inAdjacency != null && outAdjacency != null;
            this.undirected = undirected;
        }

        @Override
        public String threadName() {
            return "HugeRelationshipImport-" + batchIndex;
        }

        @Override
        public Void apply(final Statement statement) throws EntityNotFoundException {
            ReadOperations readOp = statement.readOperations();

            final RelationshipLoader loader;
            if (undirected) {
                assert outOffsets != null;
                assert outAllocator != null;

                outAllocator.prepare();
                RelationshipDeltaEncoding importer = newImporter(readOp, Direction.BOTH);
                loader = (neo, node) -> readUndirectedRelationships(
                        node,
                        neo,
                        readOp,
                        outOffsets,
                        outAllocator,
                        importer
                );
            } else {

                if (inAllocator != null) {
                    inAllocator.prepare();
                    RelationshipDeltaEncoding inImporter = newImporter(readOp, Direction.INCOMING);
                    if (outAllocator != null) {
                        outAllocator.prepare();
                        RelationshipDeltaEncoding outImporter = newImporter(readOp, Direction.OUTGOING);
                        loader = (neo, node) -> {
                            readRelationships(
                                    node,
                                    neo,
                                    readOp,
                                    Direction.OUTGOING,
                                    outOffsets,
                                    outAllocator,
                                    outImporter
                            );
                            readRelationships(
                                    node,
                                    neo,
                                    readOp,
                                    Direction.INCOMING,
                                    inOffsets,
                                    inAllocator,
                                    inImporter
                            );
                        };
                    } else {
                        loader = (neo, node) -> readRelationships(
                                node,
                                neo,
                                readOp,
                                Direction.INCOMING,
                                inOffsets,
                                inAllocator,
                                inImporter
                        );
                    }
                } else {
                    if (outAllocator != null) {
                        outAllocator.prepare();
                        RelationshipDeltaEncoding outImporter = newImporter(readOp, Direction.OUTGOING);
                        loader = (neo, node) -> readRelationships(
                                node,
                                neo,
                                readOp,
                                Direction.OUTGOING,
                                outOffsets,
                                outAllocator,
                                outImporter
                        );
                    } else {
                        loader = (neo, node) -> {
                        };
                    }
                }
            }

            NodeQueue nodes = this.nodes;
            long nodeId;
            while ((nodeId = nodes.next()) != -1L) {
                loader.apply(idMap.toOriginalNodeId(nodeId), nodeId);
                progress.relProgress();
            }
            return null;
        }

        private RelationshipDeltaEncoding newImporter(
                ReadOperations readOp,
                Direction direction) {
            if (weightId >= 0) {
                return new RelationshipDeltaEncodingWithWeights(
                        idMap,
                        direction,
                        readOp,
                        weightId,
                        weights,
                        loadsBoth);
            }
            return new RelationshipDeltaEncoding(idMap, direction);
        }

        private void readRelationships(
                long sourceGraphId,
                long sourceNodeId,
                ReadOperations readOp,
                Direction direction,
                LongArray offsets,
                ByteArray.LocalAllocator allocator,
                RelationshipDeltaEncoding delta) throws EntityNotFoundException {

            int degree = degree(sourceNodeId, readOp, direction);
            if (degree <= 0) {
                return;
            }

            RelationshipIterator rs = relationships(sourceNodeId, readOp, direction);
            delta.reset(degree, sourceGraphId);
            while (rs.hasNext()) {
                rs.relationshipVisit(rs.next(), delta);
            }

            long requiredSize = delta.applyDelta();
            degree = delta.length;
            if (degree == 0) {
                return;
            }

            long adjacencyIdx = allocator.allocate(requiredSize);
            offsets.set(sourceGraphId, adjacencyIdx);

            ByteArray.BulkAdder bulkAdder = allocator.adder;
            bulkAdder.addUnsignedInt(degree);
            long[] targets = delta.targets;
            for (int i = 0; i < degree; i++) {
                bulkAdder.addVLong(targets[i]);
            }
        }

        private void readUndirectedRelationships(
                long sourceGraphId,
                long sourceNodeId,
                ReadOperations readOp,
                LongArray offsets,
                ByteArray.LocalAllocator allocator,
                RelationshipDeltaEncoding delta) throws EntityNotFoundException {

            int degree = degree(sourceNodeId, readOp, Direction.BOTH);
            if (degree > 0) {
                delta.reset(degree, sourceGraphId);
                delta.setDirection(Direction.INCOMING);
                RelationshipIterator rs = relationships(sourceNodeId, readOp, Direction.INCOMING);
                while (rs.hasNext()) {
                    rs.relationshipVisit(rs.next(), delta);
                }
                delta.setDirection(Direction.OUTGOING);
                rs = relationships(sourceNodeId, readOp, Direction.OUTGOING);
                while (rs.hasNext()) {
                    rs.relationshipVisit(rs.next(), delta);
                }

                long requiredSize = delta.applyDelta();
                degree = delta.length;
                long adjacencyIdx = allocator.allocate(requiredSize);
                offsets.set(sourceGraphId, adjacencyIdx);

                ByteArray.BulkAdder bulkAdder = allocator.adder;
                bulkAdder.addUnsignedInt(degree);
                long[] targets = delta.targets;
                for (int i = 0; i < degree; i++) {
                    bulkAdder.addVLong(targets[i]);
                }
            }
        }

        private int degree(
                long sourceNodeId,
                ReadOperations readOp,
                Direction direction) throws EntityNotFoundException {
            return relationId == null
                    ? readOp.nodeGetDegree(sourceNodeId, direction)
                    : readOp.nodeGetDegree(sourceNodeId, direction, relationId[0]);
        }

        private RelationshipIterator relationships(
                long sourceNodeId,
                ReadOperations readOp,
                Direction direction) throws EntityNotFoundException {
            return relationId == null
                    ? readOp.nodeGetRelationships(sourceNodeId, direction)
                    : readOp.nodeGetRelationships(sourceNodeId, direction, relationId);
        }
    }

    private static class RelationshipDeltaEncoding implements RelationshipVisitor<EntityNotFoundException> {

        private final HugeIdMap idMap;
        private Direction direction;

        private long prevTarget;
        private long prevNode;
        private boolean isSorted;

        int length;
        long sourceGraphId;
        long[] targets;

        RelationshipDeltaEncoding(
                HugeIdMap idMap,
                Direction direction) {
            this.idMap = idMap;
            this.direction = direction;
            targets = new long[0];
        }

        final void reset(int degree, long sourceGraphId) {
            this.sourceGraphId = sourceGraphId;
            length = 0;
            prevTarget = -1L;
            prevNode = -1L;
            isSorted = true;
            if (targets.length < degree) {
                targets = new long[ArrayUtil.oversize(degree, Long.BYTES)];
            }
        }

        final void setDirection(Direction direction) {
            this.direction = direction;
        }

        @Override
        public final void visit(
                final long relationshipId,
                final int typeId,
                final long startNodeId,
                final long endNodeId) throws EntityNotFoundException {
            maybeVisit(
                    relationshipId,
                    direction == Direction.OUTGOING ? endNodeId : startNodeId);
        }

        long maybeVisit(
                final long relationshipId,
                final long endNodeId) throws EntityNotFoundException {
            if (endNodeId == prevNode) {
                return prevTarget;
            }
            long targetId = idMap.toHugeMappedNodeId(endNodeId);
            if (targetId == -1L) {
                return -1L;
            }

            if (isSorted && targetId < prevTarget) {
                isSorted = false;
            }
            targets[length++] = targetId;
            prevNode = endNodeId;
            prevTarget = targetId;
            return targetId;
        }

        final long applyDelta() {
            int length = this.length;
            if (length == 0) {
                return 0L;
            }

            long[] targets = this.targets;
            if (!isSorted) {
                Arrays.sort(targets, 0, length);
            }

            long delta = targets[0];
            int writePos = 1;
            long requiredBytes = 4L + DeltaEncoding.vSize(delta);  // length as full-int

            for (int i = 1; i < length; ++i) {
                long nextDelta = targets[i];
                long value = targets[writePos] = nextDelta - delta;
                if (value > 0L) {
                    ++writePos;
                    requiredBytes += DeltaEncoding.vSize(value);
                    delta = nextDelta;
                }
            }

            this.length = writePos;
            return requiredBytes;
        }
    }

    private static final class RelationshipDeltaEncodingWithWeights extends RelationshipDeltaEncoding {
        private final int weightId;
        private final HugeWeightMap weights;
        private final ReadOperations readOp;
        private final boolean isBoth;
        private final double defaultValue;

        RelationshipDeltaEncodingWithWeights(
                final HugeIdMap idMap,
                final Direction direction,
                final ReadOperations readOp,
                int weightId,
                HugeWeightMapping weights,
                boolean isBoth) {
            super(idMap, direction);
            this.readOp = readOp;
            this.isBoth = isBoth;
            if (!(weights instanceof HugeWeightMap) || weightId < 0) {
                throw new IllegalArgumentException(
                        "expected weights to be defined");
            }
            this.weightId = weightId;
            this.weights = (HugeWeightMap) weights;
            defaultValue = this.weights.defaultValue();
        }

        @Override
        long maybeVisit(
                final long relationshipId,
                final long endNodeId) throws EntityNotFoundException {
            long targetGraphId = super.maybeVisit(relationshipId, endNodeId);
            if (targetGraphId >= 0) {
                Object value = readOp.relationshipGetProperty(
                        relationshipId,
                        weightId);
                double doubleVal = RawValues.extractValue(value, defaultValue);
                if (doubleVal != defaultValue) {
                    long source = sourceGraphId;
                    long target = targetGraphId;
                    if (isBoth && source > target) {
                        target = source;
                        source = targetGraphId;
                    }
                    weights.put(source, target, doubleVal);
                }
            }
            return targetGraphId;
        }
    }
}
