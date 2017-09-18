package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.HugeWeightMap;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.LongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.function.Consumer;

final class RelationshipImporter implements Runnable, Consumer<ReadOperations> {
    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;
    private final ImportProgress progress;
    private final HugeIdMap idMap;
    private final HugeWeightMapping weights;
    private final PrimitiveLongIterable nodes;

    private final LongArray inOffsets;
    private final LongArray outOffsets;
    private final ByteArray.LocalAllocator inAllocator;
    private final ByteArray.LocalAllocator outAllocator;

    private final int[] relationId;
    private final int weightId;

    private DeltaEncodingVisitor inImporter;
    private DeltaEncodingVisitor outImporter;

    RelationshipImporter(
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            HugeWeightMapping weights,
            PrimitiveLongIterable nodes,
            LongArray inOffsets,
            LongArray outOffsets,
            ByteArray inAdjacency,
            ByteArray outAdjacency,
            int[] relationId,
            int weightId) {
        this.api = api;
        this.progress = progress;
        this.idMap = idMap;
        this.weights = weights;
        this.nodes = nodes;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.relationId = relationId;
        this.weightId = weightId;
        this.inAllocator = inAdjacency != null ? inAdjacency.newAllocator() : null;
        this.outAllocator = outAdjacency != null ? outAdjacency.newAllocator() : null;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    @Override
    public void run() {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            accept(statement.readOperations());
            tx.success();
        }
    }

    @Override
    public void accept(ReadOperations readOp) {
        PrimitiveLongIterator iterator = nodes.iterator();
        boolean loadIncoming = inAllocator != null;
        boolean loadOutgoing = outAllocator != null;

        if (loadIncoming) {
            inImporter = newImporter(
                    readOp,
                    idMap,
                    Direction.INCOMING
            );
        }
        if (loadOutgoing) {
            outImporter = newImporter(
                    readOp,
                    idMap,
                    Direction.OUTGOING
            );
        }

        while (iterator.hasNext()) {
            long nodeId = iterator.next();
            long neoId = idMap.toOriginalNodeId(nodeId);
            try {
                readNodeBatch(
                        nodeId,
                        neoId,
                        readOp,
                        loadIncoming,
                        loadOutgoing);
            } catch (EntityNotFoundException e) {
                // TODO: ignore?
                throw new RuntimeException(e);
            }
            progress.relProgress(1);
        }
    }

    private DeltaEncodingVisitor newImporter(
            ReadOperations readOp,
            HugeIdMap idMap,
            Direction direction) {
        if (weightId >= 0) {
            return new RelationshipImporterWithWeights(
                    idMap,
                    direction,
                    readOp,
                    weightId,
                    weights);
        }
        return new DeltaEncodingVisitor(idMap, direction);
    }

    private void readNodeBatch(
            long sourceGraphId,
            long sourceNodeId,
            ReadOperations readOp,
            boolean loadIncoming,
            boolean loadOutgoing) throws EntityNotFoundException {
        if (loadOutgoing) {
            readRelationshipsBatch(
                    sourceGraphId,
                    sourceNodeId,
                    readOp,
                    Direction.OUTGOING,
                    outOffsets,
                    outAllocator,
                    outImporter
            );
        }
        if (loadIncoming) {
            readRelationshipsBatch(
                    sourceGraphId,
                    sourceNodeId,
                    readOp,
                    Direction.INCOMING,
                    inOffsets,
                    inAllocator,
                    inImporter
            );
        }
    }

    private void readRelationshipsBatch(
            long sourceGraphId,
            long sourceNodeId,
            ReadOperations readOp,
            Direction direction,
            LongArray offsets,
            ByteArray.LocalAllocator allocator,
            DeltaEncodingVisitor delta) throws EntityNotFoundException {

        int degree = relationId == null
                ? readOp.nodeGetDegree(sourceNodeId, direction)
                : readOp.nodeGetDegree(
                sourceNodeId,
                direction,
                relationId[0]);

        if (degree <= 0) {
            return;
        }

        RelationshipIterator rs = relationships(
                sourceNodeId,
                readOp,
                direction);
        delta.reset(degree, sourceGraphId);
        while (rs.hasNext()) {
            rs.relationshipVisit(rs.next(), delta);
        }

        degree = delta.length;
        if (degree == 0) {
            return;
        }

        long requiredSize = delta.applyDelta();
        long adjacencyIdx = allocator.allocate(requiredSize);
        offsets.set(sourceGraphId, adjacencyIdx);

        ByteArray.BulkAdder bulkAdder = allocator.adder;
        bulkAdder.addUnsignedInt(degree);
        long[] targets = delta.targets;
        for (int i = 0; i < degree; i++) {
            bulkAdder.addVLong(targets[i]);
        }
    }

    private RelationshipIterator relationships(
            long sourceNodeId,
            ReadOperations readOp,
            Direction direction) throws EntityNotFoundException {
        return relationId == null
                ? readOp.nodeGetRelationships(sourceNodeId, direction)
                : readOp.nodeGetRelationships(
                sourceNodeId,
                direction,
                relationId);
    }

    private static class DeltaEncodingVisitor implements RelationshipVisitor<EntityNotFoundException> {
        private static final int[] encodingSizeCache;

        static {
            encodingSizeCache = new int[66];
            for (int i = 0; i < 65; i++) {
                encodingSizeCache[i] = (int) Math.ceil(i / 7.0);
            }
            encodingSizeCache[65] = 1;
        }

        private final HugeIdMap idMap;
        final Direction direction;

        long sourceGraphId;
        private long prevTarget;
        private boolean isSorted;
        private long[] targets;
        private int length;

        private DeltaEncodingVisitor(
                HugeIdMap idMap,
                Direction direction) {
            this.idMap = idMap;
            this.direction = direction;
            targets = new long[0];
        }

        final void reset(int degree, long sourceGraphId) {
            length = 0;
            this.sourceGraphId = sourceGraphId;
            prevTarget = -1L;
            isSorted = true;
            if (targets.length < degree) {
                targets = new long[ArrayUtil.oversize(degree, Long.BYTES)];
            }
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
            long targetId = idMap.toHugeMappedNodeId(endNodeId);
            if (targetId == HugeIdMap.NOT_FOUND) {
                return HugeIdMap.NOT_FOUND;
            }

            if (isSorted && targetId < prevTarget) {
                isSorted = false;
            }
            return prevTarget = targets[length++] = targetId;
        }

        final long applyDelta() {
            int length = this.length;
            long[] targets = this.targets;
            if (!isSorted) {
                Arrays.sort(targets, 0, length);
            }
            long delta = 0;
            long requiredBytes = 4;  // length as full-int
            for (int i = 0; i < length; i++) {
                long nextDelta = targets[i];
                long value = targets[i] -= delta;
                delta = nextDelta;
                int bits = Long.numberOfTrailingZeros(Long.highestOneBit(value)) + 1;
                requiredBytes += encodingSizeCache[bits];
            }
            return requiredBytes;
        }
    }

    private static final class RelationshipImporterWithWeights extends DeltaEncodingVisitor {
        private final int weightId;
        private final HugeWeightMap weights;
        private final ReadOperations readOp;

        private RelationshipImporterWithWeights(
                final HugeIdMap idMap,
                final Direction direction,
                final ReadOperations readOp,
                int weightId,
                HugeWeightMapping weights) {
            super(idMap, direction);
            this.readOp = readOp;
            if (!(weights instanceof HugeWeightMap) || weightId < 0) {
                throw new IllegalArgumentException(
                        "expected weights to be defined");
            }
            this.weightId = weightId;
            this.weights = (HugeWeightMap) weights;
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
                if (direction == Direction.OUTGOING) {
                    weights.put(
                            sourceGraphId,
                            targetGraphId,
                            value);
                } else {
                    weights.put(
                            targetGraphId,
                            sourceGraphId,
                            value);
                }
            }
            return targetGraphId;
        }
    }
}
