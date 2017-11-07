package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
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
import java.util.concurrent.atomic.AtomicInteger;

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
        int batchSize = setup.batchSize;
        AllocationTracker tracker = setup.tracker;
        HugeWeightMapping weights = hugeWeightMapping(tracker, dimensions.weightId(), setup.relationDefaultWeight);
        HugeIdMap mapping = loadHugeIdMap(tracker);
        HugeGraph graph = loadRelationships(dimensions, mapping, weights, concurrency, batchSize, tracker, progress);
        progressLogger.logDone(tracker);
        return graph;
    }

    private HugeGraph loadRelationships(
            GraphDimensions dimensions,
            HugeIdMap mapping,
            HugeWeightMapping weights,
            int concurrency,
            int batchSize,
            AllocationTracker tracker,
            ImportProgress progress) {
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
            inAdjacency.skipAllocationRegion(1);
        }
        if (setup.loadOutgoing) {
            outOffsets = LongArray.newArray(nodeCount, tracker);
            outAdjacency = ByteArray.newArray(nodeCount, tracker);
            outAdjacency.skipAllocationRegion(1);
        }
        if (setup.loadIncoming || setup.loadOutgoing) {
            // needs final b/c of reference from lambda
            final LongArray finalInOffsets = inOffsets;
            final LongArray finalOutOffsets = outOffsets;
            final ByteArray finalInAdjacency = inAdjacency;
            final ByteArray finalOutAdjacency = outAdjacency;
            final AtomicInteger batchIndex = new AtomicInteger();
            ParallelUtil.readParallel(
                    concurrency,
                    batchSize,
                    mapping,
                    (offset, nodeIds) -> new BatchImportTask(
                            api,
                            batchIndex.getAndIncrement(),
                            nodeIds,
                            progress,
                            mapping,
                            finalInOffsets,
                            finalOutOffsets,
                            finalInAdjacency,
                            finalOutAdjacency,
                            relationId,
                            weightId,
                            weights
                    ),
                    threadPool);
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

    private static final class BatchImportTask extends StatementTask<Void, EntityNotFoundException> {
        private final int batchIndex;
        private final ImportProgress progress;
        private final PrimitiveLongIterable nodes;
        private final HugeIdMap idMap;
        private final LongArray inOffsets;
        private final LongArray outOffsets;
        private final ByteArray.LocalAllocator inAllocator;
        private final ByteArray.LocalAllocator outAllocator;
        private final int[] relationId;
        private final int weightId;
        private final HugeWeightMapping weights;
        private final boolean loadsBoth;

        private DeltaEncodingVisitor inImporter;
        private DeltaEncodingVisitor outImporter;

        BatchImportTask(
                GraphDatabaseAPI api,
                int batchIndex,
                PrimitiveLongIterable nodes,
                ImportProgress progress,
                HugeIdMap idMap,
                LongArray inOffsets,
                LongArray outOffsets,
                ByteArray inAdjacency,
                ByteArray outAdjacency,
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
        }

        @Override
        public String threadName() {
            return "HugeRelationshipImport-" + batchIndex;
        }

        @Override
        public Void apply(final Statement statement) throws EntityNotFoundException {
            ReadOperations readOp = statement.readOperations();

            PrimitiveLongIterator iterator = nodes.iterator();
            boolean loadIncoming = inAllocator != null;
            boolean loadOutgoing = outAllocator != null;

            if (loadIncoming) {
                inImporter = newImporter(readOp, Direction.INCOMING);
            }
            if (loadOutgoing) {
                outImporter = newImporter(readOp, Direction.OUTGOING);
            }

            while (iterator.hasNext()) {
                long nodeId = iterator.next();
                long neoId = idMap.toOriginalNodeId(nodeId);
                readNodeBatch(
                        nodeId,
                        neoId,
                        readOp,
                        loadIncoming,
                        loadOutgoing);
                progress.relProgress();
            }
            return null;
        }

        DeltaEncodingVisitor newImporter(
                ReadOperations readOp,
                Direction direction) {
            if (weightId >= 0) {
                return new RelationshipImporterWithWeights(
                        idMap,
                        direction,
                        readOp,
                        weightId,
                        weights,
                        loadsBoth);
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
                    : readOp.nodeGetRelationships(sourceNodeId, direction, relationId);
        }
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
        private final Direction direction;

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
            if (targetId == -1L) {
                return -1L;
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
        private final boolean isBoth;
        private final double defaultValue;

        private RelationshipImporterWithWeights(
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
